using System.Collections.Concurrent;
using BrokerApp.Services;
using Grpc.Core;
using MessageHub.Contracts;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace BrokerApp;

public sealed class DeadLetterRecord
{
    public string Id { get; init; } = Guid.NewGuid().ToString();
    public string ClientId { get; init; } = string.Empty;
    public Message Message { get; init; } = new();
    public string Reason { get; init; } = string.Empty;
    public DateTime FailedAt { get; init; } = DateTime.Now;
    public int Attempts { get; init; }
}

public record BrokerActivity(string Category, string Detail, DateTime Time);
public record BrokerSubscriber(string ClientId, string Endpoint, string Topic, bool IsConnected);
public record BrokerDeadLetter(string Id, string ClientId, string Topic, string Payload, string Reason, int Attempts, DateTime FailedAt);
public record BrokerSnapshot(int SubscriberCount, int ConnectedCount, int PendingCount, IReadOnlyList<BrokerSubscriber> Subscribers, int DeadLetterCount, IReadOnlyList<BrokerDeadLetter> DeadLetters, int OrphanCount);

// nivelul de rutare si stocare al brokerului (stocare tranzienta, in colectii concurente); tot aici este gazduit
// serverul gRPC (Kestrel), iar endpoint-urile de retea si validarea datelor sunt in BrokerGrpcService
public sealed class BrokerServer : IDisposable
{
    public const int MaxMessageBytes = 64 * 1024;

    // numarul maxim de canale logice (stream-uri HTTP/2) pe o conexiune; un client foloseste cel mult cateva simultan
    public const int MaxStreamsPerConnection = 100;
    private const int MaxDeliveryAttempts = 5;

    // setul de work joburi (cron): fiecare ruleaza la DeliveryInterval si raspunde de o partitie a receiverilor
    private const int WorkerCount = 3;
    private static readonly TimeSpan AckTimeout = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan DeliveryInterval = TimeSpan.FromSeconds(2);

    private readonly ConcurrentDictionary<string, HashSet<string>> _subscriptions = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, ConcurrentQueue<Message>> _pendingMessages = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, SubscriberSession> _sessions = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _deliveryLocks = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, int> _deliveryAttempts = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, DeadLetterRecord> _deadLetterMessages = new();

    // adresa IP:port de pe care s-a abonat fiecare receiver
    private readonly ConcurrentDictionary<string, string> _clientEndpoints = new(StringComparer.OrdinalIgnoreCase);

    // mesaje publicate fara niciun abonat, pana cineva se aboneaza la topicul lor (livrarea lor vine din istoric)
    private readonly ConcurrentDictionary<string, ConcurrentQueue<Message>> _orphanMessages = new(StringComparer.OrdinalIgnoreCase);

    // istoricul complet al mesajelor publicate per topic, pentru a le livra noilor abonati
    private readonly ConcurrentDictionary<string, List<Message>> _topicHistory = new(StringComparer.OrdinalIgnoreCase);

    // inregistrarea mesajelor livrate/asignate per client, pentru a preveni duplicatele
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, byte>> _deliveredMessages = new(StringComparer.OrdinalIgnoreCase);

    // id-urile deja publicate: un Publish repetat cu acelasi id este respins
    private readonly ConcurrentDictionary<string, byte> _publishedIds = new(StringComparer.OrdinalIgnoreCase);

    // mesajele sterse din dead-letter, per client: replay-ul de la reconectare nu le mai trimite
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, byte>> _discardedMessages = new(StringComparer.OrdinalIgnoreCase);

    // mesaje readaugate din dead-letter ("Retrimite") cat timp receiverul era offline; se livreaza la reconectare
    private readonly ConcurrentDictionary<string, ConcurrentQueue<Message>> _requeuedMessages = new(StringComparer.OrdinalIgnoreCase);

    // serializeaza schimbarile de abonamente (Subscribe, AddTopics, Retrimite, stergere, oprire); publicarea si livrarea raman concurente
    private readonly object _membershipLock = new();

    // publicarile simultane iau lock-ul de citire; golirea starii ("Sterge toti", oprire) ia lock-ul de scriere,
    // ca un mesaj sa nu fie rutat pe jumatate catre o stare stearsa
    private readonly ReaderWriterLockSlim _stateLock = new();
    private readonly SemaphoreSlim _lifecycleLock = new(1, 1);
    private WebApplication? _host;
    private CancellationTokenSource? _cancellation;
    private volatile bool _isRunning;

    // apelurile Subscribe aflate inca in handler (folosit la oprire, ca statusul final sa ajunga la receiveri)
    private int _activeSubscribeCalls;

    public bool IsRunning => _isRunning;
    public event EventHandler<BrokerActivity>? Activity;

    public async Task StartAsync(int port)
    {
        await _lifecycleLock.WaitAsync();
        try
        {
            if (_isRunning)
            {
                return;
            }

            WebApplication host = BuildHost(port);
            _cancellation = new CancellationTokenSource();
            // cererile care sosesc imediat dupa deschiderea portului sunt deja acceptate
            _isRunning = true;
            try
            {
                // arunca exceptie daca portul este ocupat; brokerul ramane oprit
                await host.StartAsync();
            }
            catch
            {
                _isRunning = false;
                _cancellation.Dispose();
                _cancellation = null;
                await host.DisposeAsync();
                throw;
            }

            _host = host;
            WriteActivity("SYSTEM", $"Broker gRPC pornit pe portul {port} (HTTP/2), pe toate interfetele de retea; {WorkerCount} work joburi de livrare la {DeliveryInterval.TotalSeconds:0} s.");
            CancellationToken workerToken = _cancellation.Token;
            for (int worker = 0; worker < WorkerCount; worker++)
            {
                int index = worker;
                _ = Task.Run(() => DeliveryWorkerAsync(index, workerToken));
            }
        }
        finally
        {
            _lifecycleLock.Release();
        }
    }

    public async Task StopAsync()
    {
        await _lifecycleLock.WaitAsync();
        try
        {
            if (!_isRunning)
            {
                return;
            }

            int closedSessions;
            lock (_membershipLock)
            {
                // cererile noi primesc Unavailable, iar receiverii conectati vad inchiderea stream-ului
                _isRunning = false;
                closedSessions = CloseAllSessions(new Status(StatusCode.Unavailable, "BROKER_STOPPED: Brokerul a fost oprit."));
            }

            // si apelurile inchise chiar inainte (ex. "Sterge toti" urmat imediat de "Opreste") isi trimit statusul final
            if (closedSessions > 0 || Volatile.Read(ref _activeSubscribeCalls) > 0)
            {
                await WaitForSubscribeCallsAsync();
            }

            _cancellation?.Cancel();
            try
            {
                if (_host is not null)
                {
                    await _host.StopAsync();
                }
            }
            finally
            {
                // starea se sterge chiar daca oprirea host-ului esueaza, ca o noua pornire sa inceapa curat
                try
                {
                    if (_host is not null)
                    {
                        await _host.DisposeAsync();
                    }
                }
                finally
                {
                    _host = null;
                    _cancellation?.Dispose();
                    _cancellation = null;
                    lock (_membershipLock)
                    {
                        ClearAllState();
                    }

                    WriteActivity("SYSTEM", "Broker oprit; toata starea a fost stearsa.");
                }
            }
        }
        finally
        {
            _lifecycleLock.Release();
        }
    }

    public BrokerSnapshot GetSnapshot()
    {
        List<BrokerSubscriber> subscribers = _subscriptions
            .OrderBy(item => item.Key)
            .Select(item => new BrokerSubscriber(
                item.Key,
                _clientEndpoints.TryGetValue(item.Key, out string? endpoint) ? endpoint : "-",
                JoinTopics(item.Value),
                _sessions.ContainsKey(item.Key)))
            .ToList();

        List<BrokerDeadLetter> deadLetters = _deadLetterMessages.Values
            .OrderByDescending(record => record.FailedAt)
            .Select(record => new BrokerDeadLetter(record.Id, record.ClientId, record.Message.Topic, record.Message.Payload, record.Reason, record.Attempts, record.FailedAt))
            .ToList();

        return new BrokerSnapshot(
            subscribers.Count,
            _sessions.Count,
            _pendingMessages.Sum(item => item.Value.Count) + _requeuedMessages.Sum(item => item.Value.Count),
            subscribers,
            deadLetters.Count,
            deadLetters,
            _orphanMessages.Sum(item => item.Value.Count));
    }

    public bool RequeueDeadLetter(string id)
    {
        DeadLetterRecord? record;
        bool connected;
        lock (_membershipLock)
        {
            if (!_deadLetterMessages.TryGetValue(id, out record))
            {
                return false;
            }

            // dupa "Sterge toti" clientul nu mai exista: mesajul ar ramane blocat intr-o coada fara receiver
            if (!_subscriptions.ContainsKey(record.ClientId))
            {
                WriteActivity("DEAD_LETTER", $"{record.ClientId} nu mai este abonat; mesajul ramane in dead-letter.");
                return false;
            }

            if (!_deadLetterMessages.TryRemove(id, out record))
            {
                return false;
            }

            // "Retrimite" anuleaza un "Sterge" anterior pentru acelasi mesaj
            if (_discardedMessages.TryGetValue(record.ClientId, out ConcurrentDictionary<string, byte>? discarded))
            {
                discarded.TryRemove(record.Message.Id, out _);
            }

            // la reconectare coada receiverului se reface din istoric; un mesaj retrimis cat timp e offline
            // este pastrat separat, ca sa ajunga sigur la el (chiar daca intre timp si-a schimbat topicurile)
            connected = _sessions.ContainsKey(record.ClientId);
            ConcurrentDictionary<string, ConcurrentQueue<Message>> target = connected ? _pendingMessages : _requeuedMessages;
            target.GetOrAdd(record.ClientId, _ => new ConcurrentQueue<Message>()).Enqueue(record.Message);
        }

        if (connected)
        {
            WriteActivity("REQUEUE", $"Mesaj readaugat in coada pentru {record.ClientId}.");
            TriggerDelivery(record.ClientId);
        }
        else
        {
            WriteActivity("REQUEUE", $"Mesaj readaugat pentru {record.ClientId}; va fi livrat la reconectarea lui.");
        }

        return true;
    }

    public bool DiscardDeadLetter(string id)
    {
        if (!_deadLetterMessages.TryRemove(id, out DeadLetterRecord? record))
        {
            return false;
        }

        MarkDiscarded(record);
        WriteActivity("DEAD_LETTER", $"Mesaj sters definitiv din dead-letter; nu mai este retrimis catre {record.ClientId} nici la reconectare.");
        return true;
    }

    public void ClearDeadLetters()
    {
        if (_deadLetterMessages.IsEmpty)
        {
            return;
        }

        foreach ((string id, DeadLetterRecord record) in _deadLetterMessages)
        {
            if (_deadLetterMessages.TryRemove(id, out _))
            {
                MarkDiscarded(record);
            }
        }

        WriteActivity("DEAD_LETTER", "Coada dead-letter a fost golita.");
    }

    public void ClearAllSubscribers()
    {
        lock (_membershipLock)
        {
            if (_subscriptions.IsEmpty && _sessions.IsEmpty && _pendingMessages.IsEmpty && _topicHistory.IsEmpty && _orphanMessages.IsEmpty)
            {
                return;
            }

            // receiverii primesc Aborted si nu se mai reconecteaza automat
            CloseAllSessions(new Status(StatusCode.Aborted, "SUBSCRIPTIONS_CLEARED: Abonamentele au fost sterse de broker."));
            _stateLock.EnterWriteLock();
            try
            {
                // mesajele retrimise catre receiveri offline revin in dead-letter (care supravietuieste), nu dispar
                ReturnRequeuedMessagesToDeadLetter();
                _subscriptions.Clear();
                _pendingMessages.Clear();
                _deliveryAttempts.Clear();
                // istoricul dispare, deci si mesajele fara abonat (ele se livrau din istoric)
                _topicHistory.Clear();
                _orphanMessages.Clear();
                _publishedIds.Clear();
                _deliveredMessages.Clear();
                _discardedMessages.Clear();
                _clientEndpoints.Clear();
            }
            finally
            {
                _stateLock.ExitWriteLock();
            }
        }

        WriteActivity("SYSTEM", "Toti abonatii au fost eliminati; brokerul a fost resetat la 0 receivere.");
    }

    public bool SimulateDeliveryFailure(string clientId)
    {
        SemaphoreSlim deliveryLock = _deliveryLocks.GetOrAdd(clientId, _ => new SemaphoreSlim(1, 1));

        // nu concureaza cu o livrare in curs catre acelasi receiver
        if (!deliveryLock.Wait(0))
        {
            WriteActivity("SYSTEM", $"O livrare catre {clientId} este in curs; incercati din nou peste cateva secunde.");
            return false;
        }

        try
        {
            if (!_pendingMessages.TryGetValue(clientId, out ConcurrentQueue<Message>? queue) || !queue.TryPeek(out Message? message))
            {
                WriteActivity("SYSTEM", $"Nu exista niciun mesaj pending pentru {clientId} de simulat ca esuat.");
                return false;
            }

            HandleDeliveryFailure(clientId, queue, message, "Esec simulat manual (test).");
            return true;
        }
        finally
        {
            deliveryLock.Release();
        }
    }

    // ---- operatii apelate de BrokerGrpcService ----

    public BrokerReply Publish(Message message, string endpoint)
    {
        EnsureRunning();

        _stateLock.EnterReadLock();
        try
        {
            return Route(message, endpoint);
        }
        finally
        {
            _stateLock.ExitReadLock();
        }
    }

    // pune mesajul in istoric si in coada fiecarui abonat al topicului; ruleaza sub lock-ul de citire al starii
    private BrokerReply Route(Message message, string endpoint)
    {
        // acelasi id publicat de doua ori ar intra de doua ori in istoric, dar deduplicarea nu l-ar mai livra nimanui
        if (!_publishedIds.TryAdd(message.Id, 0))
        {
            throw new RpcException(new Status(StatusCode.AlreadyExists,
                $"DUPLICATE_MESSAGE: Un mesaj cu Id-ul {message.Id} a fost deja publicat; fiecare mesaj trebuie sa aiba un Id nou."));
        }

        // pastreaza mesajul in istoricul topicului pentru viitorii abonati
        _topicHistory.AddOrUpdate(
            message.Topic,
            _ => new List<Message> { message },
            (_, list) => { lock (list) { list.Add(message); } return list; });

        // rutarea: topicul din corpul mesajului decide receiverii
        string[] recipients = _subscriptions
            .Where(item => item.Value.Contains(message.Topic))
            .Select(item => item.Key)
            .ToArray();

        foreach (string clientId in recipients)
        {
            // mesajul intra o singura data in coada receiverului, chiar daca replay-ul istoricului l-a adaugat deja
            if (_deliveredMessages.GetOrAdd(clientId, _ => NewIdSet()).TryAdd(message.Id, 0))
            {
                _pendingMessages.GetOrAdd(clientId, _ => new ConcurrentQueue<Message>()).Enqueue(message);
            }

            TriggerDelivery(clientId);
        }

        string source = $"{message.SenderId} ({endpoint})";
        if (recipients.Length == 0)
        {
            _orphanMessages.GetOrAdd(message.Topic, _ => new ConcurrentQueue<Message>()).Enqueue(message);
            WriteActivity("PUBLISH", $"Topic „{message.Topic}” publicat de {source}, dar fara niciun abonat momentan (mesaj pastrat pentru viitor).");
            return Reply("PUBLISH_ACCEPTED", "Mesaj acceptat, dar momentan nu exista niciun abonat pe acest topic; va fi livrat cand cineva se aboneaza.", endpoint);
        }

        WriteActivity("PUBLISH", $"Topic „{message.Topic}” de la {source} trimis catre {recipients.Length} abonat(i).");
        return Reply("PUBLISH_ACCEPTED", "Mesaj validat si stocat pentru livrare.", endpoint);
    }

    public List<string> GetActiveTopics()
    {
        EnsureRunning();

        return _sessions.Keys
            .SelectMany(clientId => _subscriptions.TryGetValue(clientId, out HashSet<string>? topics) ? topics : Enumerable.Empty<string>())
            .Where(topic => !string.IsNullOrWhiteSpace(topic))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(topic => topic, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    // inregistreaza un receiver conectat prin stream-ul Subscribe (echivalentul SUBSCRIBE din Partea 1)
    public async Task<SubscriberSession> OpenSessionAsync(string clientId, HashSet<string> topics, string endpoint, IServerStreamWriter<BrokerFrame> writer, CancellationToken callCancellation)
    {
        // o livrare inca in curs catre sesiunea veche a aceluiasi client (ex. receiverul a cazut chiar acum) isi termina
        // intai tratarea esecului: incercarea e numarata si, la a 5-a, mesajul e deja in dead-letter cand ruleaza replay-ul
        SemaphoreSlim deliveryLock = _deliveryLocks.GetOrAdd(clientId, _ => new SemaphoreSlim(1, 1));
        bool locked = await deliveryLock.WaitAsync(TimeSpan.FromSeconds(3), callCancellation);
        try
        {
            return OpenSessionCore(clientId, topics, endpoint, writer, callCancellation);
        }
        finally
        {
            if (locked)
            {
                deliveryLock.Release();
            }
        }
    }

    private SubscriberSession OpenSessionCore(string clientId, HashSet<string> topics, string endpoint, IServerStreamWriter<BrokerFrame> writer, CancellationToken callCancellation)
    {
        var session = new SubscriberSession(clientId, endpoint, writer, callCancellation);
        lock (_membershipLock)
        {
            EnsureRunning();

            // sesiunile se adauga numai sub acest lock, deci verificarea nu poate fi depasita de alt Subscribe
            if (_sessions.ContainsKey(clientId))
            {
                throw new RpcException(new Status(StatusCode.AlreadyExists,
                    $"CLIENT_ID_TAKEN: Numele de client '{clientId}' este deja folosit de un receiver conectat activ. Alege alt Client ID."));
            }

            // abonarea, resetarea si replay-ul sunt atomice fata de Publish: altfel un mesaj publicat exact intre
            // resetari ar fi marcat ca livrat, dar pus in coada veche (aruncata)
            _stateLock.EnterWriteLock();
            try
            {
                _subscriptions[clientId] = topics;
                _clientEndpoints[clientId] = endpoint;

                // la reconectare, reseteaza evidenta de deduplicare si coada veche
                // pentru a permite replay-ul complet al istoricului; incercarile esuate se pastreaza, ca un mesaj
                // care face receiverul sa cada la fiecare livrare sa ajunga totusi in dead-letter
                _deliveredMessages.TryRemove(clientId, out _);
                _pendingMessages.TryRemove(clientId, out _);

                foreach (string topic in topics)
                {
                    ClaimOrphanMessagesForTopic(clientId, topic);
                }
                // livreaza istoricul mesajelor anterioare pentru topicele abonate
                ReplayHistoryForClient(clientId, topics);
                EnqueueRequeuedMessages(clientId);
            }
            finally
            {
                _stateLock.ExitWriteLock();
            }

            // sesiunea devine vizibila livrarilor abia dupa ce coada noua este pregatita
            _sessions[clientId] = session;
            Interlocked.Increment(ref _activeSubscribeCalls);
        }

        WriteActivity("SUBSCRIBE", $"{clientId} ({endpoint}) este abonat la „{JoinTopics(topics)}”.");
        return session;
    }

    public static BrokerReply CreateSubscribedReply(HashSet<string> topics, string endpoint) =>
        Reply("SUBSCRIBED", $"Abonat la topicurile: {JoinTopics(topics)}.", endpoint);

    public BrokerReply AddTopics(string clientId, HashSet<string> requestedTopics, string endpoint)
    {
        string[] newTopics;
        HashSet<string> topics;
        lock (_membershipLock)
        {
            EnsureRunning();
            if (!_subscriptions.TryGetValue(clientId, out HashSet<string>? existingTopics))
            {
                throw new RpcException(new Status(StatusCode.NotFound,
                    "UNKNOWN_CLIENT: Clientul trebuie sa fie deja abonat (Subscribe) inainte de a adauga topicuri noi."));
            }

            // identificarea receiverului: cererea trebuie sa vina pe aceeasi conexiune ca stream-ul lui Subscribe
            if (_sessions.TryGetValue(clientId, out SubscriberSession? session) && session.Endpoint != endpoint)
            {
                throw new RpcException(new Status(StatusCode.PermissionDenied,
                    $"NOT_OWNER: Abonamentul lui '{clientId}' poate fi modificat doar de receiverul conectat cu acest ClientId."));
            }

            newTopics = requestedTopics.Where(topic => !existingTopics.Contains(topic)).ToArray();
            if (newTopics.Length == 0)
            {
                throw new RpcException(new Status(StatusCode.AlreadyExists,
                    $"ALREADY_SUBSCRIBED: Esti deja abonat la: {JoinTopics(requestedTopics)}. Nu s-a adaugat nimic nou."));
            }

            topics = new HashSet<string>(existingTopics, StringComparer.OrdinalIgnoreCase);
            topics.UnionWith(newTopics);
            _subscriptions[clientId] = topics;

            foreach (string topic in newTopics)
            {
                ClaimOrphanMessagesForTopic(clientId, topic);
            }
            // livreaza istoricul mesajelor anterioare pentru topicele nou adaugate
            ReplayHistoryForClient(clientId, newTopics);
        }

        string joinedNewTopics = JoinTopics(newTopics);
        WriteActivity("SUBSCRIBE", $"{clientId} a adaugat topicul(urile) „{joinedNewTopics}”. Total abonamente: {JoinTopics(topics)}.");
        TriggerDelivery(clientId);
        return Reply("TOPIC_ADDED", $"Adaugat: {joinedNewTopics}. Topicuri curente: {JoinTopics(topics)}.", endpoint);
    }

    public void Acknowledge(SubscriberSession session, string messageId)
    {
        if (!session.TryAcknowledge(messageId))
        {
            WriteActivity("ACK", $"ACK de la {session.ClientId} ignorat: mesajul {messageId} nu mai era asteptat (confirmare intarziata).");
        }
    }

    // apelat de handlerul Subscribe la final, dupa drenarea scrierilor (pereche cu OpenSessionAsync reusit)
    public void EndSubscribeCall() => Interlocked.Decrement(ref _activeSubscribeCalls);

    // scoate receiverul dintre cei conectati; sterge doar sesiunea data, nu una noua deschisa intre timp cu acelasi ClientId
    public void CloseSession(SubscriberSession session)
    {
        session.Close();
        if (_sessions.TryRemove(new KeyValuePair<string, SubscriberSession>(session.ClientId, session)))
        {
            WriteActivity("OFFLINE", $"{session.ClientId} este deconectat.");
        }
    }

    // livrarea porneste separat: apelantul (ex. handlerul Subscribe, care citeste ACK-urile) nu o asteapta
    public void TriggerDelivery(string clientId) => _ = Task.Run(() => DeliverPendingMessagesAsync(clientId, worker: null));

    public void ReportRejectedCall(string method, string endpoint, Status status) =>
        WriteActivity("ERROR", $"{method} de la {endpoint} respins ({status.StatusCode}): {status.Detail}");

    public void ReportUnexpectedError(string method, Exception ex) =>
        WriteActivity("ERROR", $"Eroare neasteptata in {method}: {ex.Message} Brokerul ramane activ.");

    // ---- livrare ----

    // worker = indexul work job-ului care a pornit livrarea, sau null cand livrarea vine dintr-un eveniment (Publish, Subscribe...)
    private async Task DeliverPendingMessagesAsync(string clientId, int? worker)
    {
        SemaphoreSlim deliveryLock = _deliveryLocks.GetOrAdd(clientId, _ => new SemaphoreSlim(1, 1));
        if (worker is null)
        {
            await deliveryLock.WaitAsync();
        }
        else if (!deliveryLock.Wait(0))
        {
            // work job-ul sare peste receiverii serviti deja de alta livrare
            return;
        }

        try
        {
            if (!_isRunning || !_sessions.TryGetValue(clientId, out SubscriberSession? session) ||
                !_pendingMessages.TryGetValue(clientId, out ConcurrentQueue<Message>? queue))
            {
                return;
            }

            if (worker is not null && !queue.IsEmpty)
            {
                WriteActivity("WORKER", $"Work job {worker + 1}: reia livrarea catre {clientId} ({queue.Count} mesaj(e) in asteptare).");
            }

            while (!session.IsClosed && queue.TryPeek(out Message? message))
            {
                if (!await TryDeliverAsync(session, queue, message))
                {
                    break;
                }
            }
        }
        catch (Exception ex)
        {
            WriteActivity("ERROR", $"Eroare neasteptata la livrarea catre {clientId}: {ex.Message}");
        }
        finally
        {
            deliveryLock.Release();
        }
    }

    // trimite mesajul pe stream si asteapta ACK-ul; true daca receiverul l-a confirmat
    private async Task<bool> TryDeliverAsync(SubscriberSession session, ConcurrentQueue<Message> queue, Message message)
    {
        string clientId = session.ClientId;
        Task ack = session.ExpectAck(message.Id);
        try
        {
            await session.SendMessageAsync(message);
            await ack.WaitAsync(AckTimeout, session.Token);
        }
        catch (TimeoutException)
        {
            // receiverul este conectat, dar nu a confirmat: mesajul ramane pending, work job-ul reincearca
            session.ClearExpectedAck();
            HandleDeliveryFailure(clientId, queue, message, "Nu s-a primit confirmarea ACK in 5 secunde.");
            return false;
        }
        catch (Exception ex) when (ex is IOException or InvalidOperationException or OperationCanceledException)
        {
            CloseSession(session);
            // o sesiune inchisa de broker (oprire, "Sterge toti") nu este un esec al receiverului
            if (session.CloseStatus is null)
            {
                HandleDeliveryFailure(clientId, queue, message, "Stream-ul catre receiver s-a inchis.");
            }

            return false;
        }

        queue.TryDequeue(out _);
        _deliveryAttempts.TryRemove(AttemptKey(clientId, message.Id), out _);
        WriteActivity("DELIVERED", $"Mesaj livrat si confirmat de {clientId}.");
        return true;
    }

    private void HandleDeliveryFailure(string clientId, ConcurrentQueue<Message> queue, Message message, string reason)
    {
        if (!_isRunning)
        {
            return;
        }

        string attemptKey = AttemptKey(clientId, message.Id);
        int attempts = _deliveryAttempts.AddOrUpdate(attemptKey, 1, (_, count) => count + 1);
        if (attempts < MaxDeliveryAttempts)
        {
            WriteActivity("WAITING", $"{clientId} nu a confirmat mesajul (incercarea {attempts}/{MaxDeliveryAttempts}): {reason} Mesajul ramane pending.");
            return;
        }

        _deliveryAttempts.TryRemove(attemptKey, out _);
        if (queue.TryPeek(out Message? head) && ReferenceEquals(head, message) && queue.TryDequeue(out _))
        {
            var record = new DeadLetterRecord
            {
                ClientId = clientId,
                Message = message,
                Reason = reason,
                FailedAt = DateTime.Now,
                Attempts = attempts
            };
            _deadLetterMessages[record.Id] = record;
            WriteActivity("DEAD_LETTER", $"Mesaj catre {clientId} mutat in dead-letter dupa {attempts} incercari esuate ({reason})");
        }
    }

    // un work job (cron): la fiecare 2 secunde porneste concurent cate o livrare pentru fiecare receiver conectat
    // din partitia lui; joburile pornesc decalat, ca sa nu ruleze toate in acelasi moment
    private async Task DeliveryWorkerAsync(int index, CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(DeliveryInterval * index / WorkerCount, cancellationToken);
            while (!cancellationToken.IsCancellationRequested)
            {
                foreach (string clientId in _sessions.Keys)
                {
                    if (WorkerFor(clientId) == index)
                    {
                        _ = Task.Run(() => DeliverPendingMessagesAsync(clientId, index), cancellationToken);
                    }
                }

                await Task.Delay(DeliveryInterval, cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            // brokerul a fost oprit
        }
    }

    // partitionarea receiverilor intre work joburi (stabila cat ruleaza procesul)
    private static int WorkerFor(string clientId) =>
        (int)((uint)StringComparer.OrdinalIgnoreCase.GetHashCode(clientId) % WorkerCount);

    // ---- utilitare ----

    private WebApplication BuildHost(int port)
    {
        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.Services.Configure<HostOptions>(options => options.ShutdownTimeout = TimeSpan.FromSeconds(3));
        builder.WebHost.ConfigureKestrel(options =>
        {
            // gRPC foloseste HTTP/2; fara TLS, pe toate interfetele de retea
            options.ListenAnyIP(port, listen => listen.Protocols = HttpProtocols.Http2);
            // numarul de canale logice pe conexiune se stabileste la crearea serverului
            options.Limits.Http2.MaxStreamsPerConnection = MaxStreamsPerConnection;
            // ping-uri HTTP/2: un receiver cazut fara sa inchida conexiunea este detectat in ~25 de secunde
            options.Limits.Http2.KeepAlivePingDelay = TimeSpan.FromSeconds(15);
            options.Limits.Http2.KeepAlivePingTimeout = TimeSpan.FromSeconds(10);
        });

        builder.Services.AddSingleton(this);
        builder.Services.AddGrpc(options =>
        {
            // cererile respinse ajung in jurnal, iar exceptiile neprevazute nu pot opri brokerul
            options.Interceptors.Add<BrokerErrorInterceptor>();
            options.MaxReceiveMessageSize = MaxMessageBytes;
        });
        // descrierea serviciului pentru Postman / grpcurl (testarea validarii)
        builder.Services.AddGrpcReflection();

        WebApplication host = builder.Build();
        host.MapGrpcService<BrokerGrpcService>();
        host.MapGrpcReflectionService();
        return host;
    }

    private int CloseAllSessions(Status status)
    {
        int closed = 0;
        foreach (SubscriberSession session in _sessions.Values)
        {
            session.Close(status);
            _sessions.TryRemove(new KeyValuePair<string, SubscriberSession>(session.ClientId, session));
            closed++;
        }

        return closed;
    }

    // la oprire: statusul final (Unavailable) trebuie sa ajunga la receiveri inainte ca Kestrel sa inchida conexiunile;
    // un receiver aglomerat (mii de mesaje in buffer, ACK-uri inca in drum) l-ar putea pierde la inchiderea TCP
    private async Task WaitForSubscribeCallsAsync()
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(2.5);
        while (Volatile.Read(ref _activeSubscribeCalls) > 0 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(20);
        }

        await Task.Delay(500);
    }

    private void ClearAllState()
    {
        _stateLock.EnterWriteLock();
        try
        {
            _subscriptions.Clear();
            _pendingMessages.Clear();
            _requeuedMessages.Clear();
            _deliveryAttempts.Clear();
            _deadLetterMessages.Clear();
            _orphanMessages.Clear();
            _topicHistory.Clear();
            _deliveredMessages.Clear();
            _clientEndpoints.Clear();
            _publishedIds.Clear();
            _discardedMessages.Clear();
        }
        finally
        {
            _stateLock.ExitWriteLock();
        }
    }

    private void EnsureRunning()
    {
        if (!_isRunning)
        {
            throw new RpcException(new Status(StatusCode.Unavailable, "BROKER_STOPPED: Brokerul este oprit."));
        }
    }

    private void ReturnRequeuedMessagesToDeadLetter()
    {
        foreach ((string clientId, ConcurrentQueue<Message> requeued) in _requeuedMessages)
        {
            foreach (Message message in requeued)
            {
                var record = new DeadLetterRecord
                {
                    ClientId = clientId,
                    Message = message,
                    Reason = "Retrimis catre un receiver offline, dar abonamentele au fost sterse („Sterge toti”).",
                    FailedAt = DateTime.Now
                };
                _deadLetterMessages[record.Id] = record;
            }
        }

        _requeuedMessages.Clear();
    }

    // mesajele retrimise din dead-letter cat timp receiverul era offline intra in coada dupa replay (fara duplicate)
    private void EnqueueRequeuedMessages(string clientId)
    {
        if (!_requeuedMessages.TryRemove(clientId, out ConcurrentQueue<Message>? requeued))
        {
            return;
        }

        ConcurrentQueue<Message> queue = _pendingMessages.GetOrAdd(clientId, _ => new ConcurrentQueue<Message>());
        ConcurrentDictionary<string, byte> clientDelivered = _deliveredMessages.GetOrAdd(clientId, _ => NewIdSet());
        foreach (Message message in requeued)
        {
            if (clientDelivered.TryAdd(message.Id, 0))
            {
                queue.Enqueue(message);
            }
        }
    }

    private void ReplayHistoryForClient(string clientId, IEnumerable<string> topics)
    {
        int replayed = 0;
        ConcurrentQueue<Message> queue = _pendingMessages.GetOrAdd(clientId, _ => new ConcurrentQueue<Message>());
        ConcurrentDictionary<string, byte> clientDelivered = _deliveredMessages.GetOrAdd(clientId, _ => NewIdSet());

        // mesajele aflate in dead-letter sau sterse de acolo pentru acest client nu se retrimit automat;
        // decizia ramane la butoanele "Retrimite" / "Sterge"
        foreach (DeadLetterRecord record in _deadLetterMessages.Values)
        {
            if (string.Equals(record.ClientId, clientId, StringComparison.OrdinalIgnoreCase))
            {
                clientDelivered.TryAdd(record.Message.Id, 0);
            }
        }

        if (_discardedMessages.TryGetValue(clientId, out ConcurrentDictionary<string, byte>? discarded))
        {
            foreach (string messageId in discarded.Keys)
            {
                clientDelivered.TryAdd(messageId, 0);
            }
        }

        foreach (string topic in topics)
        {
            if (_topicHistory.TryGetValue(topic, out List<Message>? history))
            {
                lock (history)
                {
                    foreach (Message message in history)
                    {
                        if (clientDelivered.TryAdd(message.Id, 0))
                        {
                            queue.Enqueue(message);
                            replayed++;
                        }
                    }
                }
            }
        }

        if (replayed > 0)
        {
            WriteActivity("REPLAY", $"{clientId} a primit {replayed} mesaj(e) din istoricul topicelor.");
        }
    }

    private void ClaimOrphanMessagesForTopic(string clientId, string topic)
    {
        if (!_orphanMessages.TryRemove(topic, out _))
        {
            return;
        }

        WriteActivity("PUBLISH", $"Topic „{topic}” are acum cel putin un abonat ({clientId}); starea fara abonati a fost resetata.");
    }

    private void MarkDiscarded(DeadLetterRecord record) =>
        _discardedMessages.GetOrAdd(record.ClientId, _ => NewIdSet()).TryAdd(record.Message.Id, 0);

    private static BrokerReply Reply(string code, string detail, string endpoint) =>
        new() { Code = code, Detail = detail, ClientEndpoint = endpoint };

    private static string JoinTopics(IEnumerable<string> topics) =>
        string.Join(", ", topics.OrderBy(topic => topic, StringComparer.OrdinalIgnoreCase));

    private static string AttemptKey(string clientId, string messageId) => $"{clientId}:{messageId}";

    private static ConcurrentDictionary<string, byte> NewIdSet() => new(StringComparer.OrdinalIgnoreCase);

    private void WriteActivity(string category, string detail) => Activity?.Invoke(this, new BrokerActivity(category, detail, DateTime.Now));

    public void Dispose()
    {
        // apelat dupa StopAsync, la inchiderea ferestrei
        _cancellation?.Dispose();
    }
}
