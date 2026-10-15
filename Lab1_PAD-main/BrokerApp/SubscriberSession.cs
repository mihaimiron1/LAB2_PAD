using Grpc.Core;
using MessageHub.Contracts;

namespace BrokerApp;

// un receiver conectat prin stream-ul Subscribe: scrierile pe stream si confirmarea asteptata de la el
public sealed class SubscriberSession
{
    private readonly IServerStreamWriter<BrokerFrame> _writer;
    private readonly CancellationTokenSource _cancellation;

    // gRPC permite un singur WriteAsync activ pe stream; lock-ul este creat ocupat si eliberat dupa raspunsul SUBSCRIBED,
    // deci niciun mesaj nu poate pleca inaintea raspunsului
    private readonly SemaphoreSlim _writeLock = new(0, 1);
    private readonly object _stateLock = new();
    private string? _expectedAckId;
    private TaskCompletionSource? _pendingAck;
    private Status? _closeStatus;

    public SubscriberSession(string clientId, string endpoint, IServerStreamWriter<BrokerFrame> writer, CancellationToken callCancellation)
    {
        ClientId = clientId;
        Endpoint = endpoint;
        _writer = writer;
        _cancellation = CancellationTokenSource.CreateLinkedTokenSource(callCancellation);
        Token = _cancellation.Token;
    }

    public string ClientId { get; }

    public string Endpoint { get; }

    // anulat cand receiverul se deconecteaza sau cand brokerul inchide sesiunea
    public CancellationToken Token { get; }

    public bool IsClosed => Token.IsCancellationRequested;

    // statusul cu care brokerul a inchis sesiunea (Unavailable la oprire, Aborted la "Sterge toti")
    public Status? CloseStatus
    {
        get
        {
            lock (_stateLock)
            {
                return _closeStatus;
            }
        }
    }

    // primul cadru de pe stream; lock-ul de scriere este ocupat de la crearea sesiunii
    public async Task SendReplyAsync(BrokerReply reply)
    {
        try
        {
            // sesiunea poate fi inchisa (ex. "Sterge toti") chiar inainte de raspuns
            Token.ThrowIfCancellationRequested();
            await _writer.WriteAsync(new BrokerFrame { Reply = reply });
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public async Task SendMessageAsync(Message message)
    {
        await _writeLock.WaitAsync(Token);
        try
        {
            Token.ThrowIfCancellationRequested();
            // fara token: in grpc-dotnet anularea unei scrieri reseteaza tot stream-ul (RST_STREAM),
            // iar receiverul ar pierde statusul final trimis de broker (ex. Aborted la "Sterge toti")
            await _writer.WriteAsync(new BrokerFrame { Message = message });
        }
        finally
        {
            _writeLock.Release();
        }
    }

    // la sfarsitul apelului: asteapta scrierea in curs si blocheaza scrierile urmatoare,
    // ca statusul final sa plece dupa ultimul mesaj complet
    public Task<bool> DrainWritesAsync(TimeSpan timeout) => _writeLock.WaitAsync(timeout);

    // se inregistreaza inainte de trimitere, ca o confirmare foarte rapida sa nu se piarda
    public Task ExpectAck(string messageId)
    {
        var pendingAck = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (_stateLock)
        {
            _expectedAckId = messageId;
            _pendingAck = pendingAck;
        }

        return pendingAck.Task;
    }

    // apelat din bucla care citeste stream-ul; ACK-urile intarziate sau necunoscute sunt ignorate
    public bool TryAcknowledge(string messageId)
    {
        TaskCompletionSource? pendingAck;
        lock (_stateLock)
        {
            if (_pendingAck is null || !string.Equals(_expectedAckId, messageId, StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            pendingAck = _pendingAck;
            _pendingAck = null;
            _expectedAckId = null;
        }

        return pendingAck.TrySetResult();
    }

    public void ClearExpectedAck()
    {
        lock (_stateLock)
        {
            _pendingAck = null;
            _expectedAckId = null;
        }
    }

    // opreste stream-ul: bucla de citire din handler se termina, iar livrarea in curs nu mai asteapta ACK-ul
    public void Close(Status? status = null)
    {
        TaskCompletionSource? pendingAck;
        lock (_stateLock)
        {
            _closeStatus ??= status;
            pendingAck = _pendingAck;
            _pendingAck = null;
            _expectedAckId = null;
        }

        pendingAck?.TrySetCanceled();
        _cancellation.Cancel();
    }
}
