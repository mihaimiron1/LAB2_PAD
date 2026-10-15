using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Avalonia.Controls;
using Avalonia.Interactivity;
using Avalonia.Media;
using Avalonia.Threading;
using Grpc.Core;
using Grpc.Net.Client;
using MessageHub.Configuration;
using MessageHub.Contracts;
using Timestamp = Google.Protobuf.WellKnownTypes.Timestamp;

namespace ReceiverApp
{
    public class ReceivedEntry
    {
        public string Topic { get; set; } = string.Empty;
        public string Payload { get; set; } = string.Empty;
        public string Time { get; set; } = string.Empty;
    }

    public partial class MainWindow : Window
    {
        private const int MaxFeedEntries = 200;
        private static readonly TimeSpan FirstFrameTimeout = TimeSpan.FromSeconds(5);
        private static readonly TimeSpan ReconnectDelay = TimeSpan.FromSeconds(3);

        private readonly ObservableCollection<ReceivedEntry> _feed = new();

        // mesajele primite pe firul de retea se afiseaza in loturi, ca un replay mare sa nu blocheze interfata
        private readonly ConcurrentQueue<(Message Message, bool Acknowledged)> _incomingMessages = new();
        private readonly DispatcherTimer _feedTimer;

        // id-urile mesajelor deja afisate (doar pe firul UI); livrarea este at-least-once
        private readonly HashSet<string> _receivedIds = new(StringComparer.OrdinalIgnoreCase);

        // topicurile abonamentului; citite si de bucla de reconectare, deci protejate de lock
        private readonly object _topicsLock = new();
        private readonly HashSet<string> _subscribedTopics = new(StringComparer.OrdinalIgnoreCase);

        // starea conexiunii (doar pe firul UI); _connection este anulat la "Deconecteaza" sau la inchiderea ferestrei
        private GrpcChannel? _channel;
        private MessageBroker.MessageBrokerClient? _client;
        private CancellationTokenSource? _connection;
        private string _brokerUrl = string.Empty;
        private string _clientId = string.Empty;

        // test: receiverul nu confirma mesajele, ca timeout-ul ACK, reincercarile si dead-letter-ul sa se vada din interfata
        private volatile bool _skipAcks;

        public MainWindow()
        {
            InitializeComponent();
            FeedList.ItemsSource = _feed;
            BrokerAddressBox.Text = $"{EnvironmentSettings.BrokerHost}:{EnvironmentSettings.BrokerPort}";
            _feedTimer = new DispatcherTimer { Interval = TimeSpan.FromMilliseconds(100) };
            _feedTimer.Tick += (_, _) => FlushMessages();
            _feedTimer.Start();
            Closed += (_, _) =>
            {
                _feedTimer.Stop();
                _connection?.Cancel();
            };
        }

        private void OnConnectClick(object? sender, RoutedEventArgs e)
        {
            if (_connection is not null)
            {
                // "Deconecteaza": opreste stream-ul si reconectarea automata
                ConnectButton.IsEnabled = false;
                _connection.Cancel();
                return;
            }

            if (!EnvironmentSettings.TryParseBrokerAddress(BrokerAddressBox.Text, out string brokerHost, out int brokerPort))
            {
                SetStatus("Adresa brokerului este invalida (ex: 192.168.1.10:5000).", Brushes.OrangeRed);
                return;
            }

            try
            {
                _channel = BrokerChannelFactory.Create(brokerHost, brokerPort);
            }
            catch (Exception ex)
            {
                SetStatus($"Adresa brokerului este invalida: {ex.Message}", Brushes.OrangeRed);
                return;
            }

            // ClientId-ul si topicurile sunt validate de broker; motivul unei respingeri apare in status
            _clientId = ClientIdBox.Text?.Trim() ?? string.Empty;
            lock (_topicsLock)
            {
                _subscribedTopics.Clear();
                _subscribedTopics.UnionWith(ParseTopics(TopicBox.Text));
            }

            _client = new MessageBroker.MessageBrokerClient(_channel);
            _brokerUrl = BrokerChannelFactory.GetAddress(brokerHost, brokerPort);
            _connection = new CancellationTokenSource();

            BrokerAddressBox.IsEnabled = false;
            ClientIdBox.IsEnabled = false;
            TopicBox.IsEnabled = false;
            ConnectButton.Content = "Deconecteaza";
            SetStatus("Se conecteaza...", Brushes.Gray);

            MessageBroker.MessageBrokerClient client = _client;
            string clientId = _clientId;
            CancellationToken token = _connection.Token;
            _ = Task.Run(() => RunSubscriptionAsync(client, clientId, token));
        }

        private async void OnAddTopicClick(object? sender, RoutedEventArgs e)
        {
            string[] newTopics = ParseTopics(NewTopicBox.Text);
            if (newTopics.Length == 0 || _client is null)
            {
                return;
            }

            AddTopicButton.IsEnabled = false;
            try
            {
                // acelasi canal HTTP/2 ca stream-ul Subscribe (in Partea 1 era nevoie de o conexiune TCP separata)
                var request = new AddTopicsRequest { ClientId = _clientId };
                request.Topics.AddRange(newTopics);
                BrokerReply reply = await _client.AddTopicsAsync(request, deadline: DateTime.UtcNow.AddSeconds(5));

                lock (_topicsLock)
                {
                    _subscribedTopics.UnionWith(newTopics);
                }

                NewTopicBox.Text = string.Empty;
                RefreshTopicsText();
                SetStatus(reply.Detail, Brushes.SeaGreen);
            }
            catch (RpcException ex)
            {
                SetStatus(BrokerChannelFactory.Describe(ex), Brushes.OrangeRed);
            }
            finally
            {
                AddTopicButton.IsEnabled = NewTopicBox.IsEnabled;
            }
        }

        private void OnSkipAckChanged(object? sender, RoutedEventArgs e) => _skipAcks = SkipAckCheckBox.IsChecked == true;

        // canalul de lunga durata catre broker; dupa o abonare reusita, la caderea brokerului se reconecteaza automat
        // cu acelasi ClientId si aceleasi topicuri (inclusiv cele adaugate ulterior)
        private async Task RunSubscriptionAsync(MessageBroker.MessageBrokerClient client, string clientId, CancellationToken token)
        {
            bool subscribed = false;
            int attempt = 0;
            string finalStatus = "Deconectat de la broker.";
            IBrush finalColor = Brushes.Gray;

            while (!token.IsCancellationRequested)
            {
                string reason;
                try
                {
                    reason = await ListenAsync(client, clientId, token, () =>
                    {
                        subscribed = true;
                        attempt = 0;
                    });
                }
                catch (Exception) when (token.IsCancellationRequested)
                {
                    break;
                }
                catch (RpcException ex) when (IsFinal(ex, subscribed))
                {
                    finalStatus = DescribeStop(ex);
                    finalColor = Brushes.OrangeRed;
                    break;
                }
                catch (RpcException ex)
                {
                    reason = BrokerChannelFactory.Describe(ex);
                }
                catch (Exception ex)
                {
                    reason = $"Eroare de conexiune: {ex.Message}";
                }

                if (!subscribed)
                {
                    // inainte de prima abonare reusita nu se reincearca: probabil adresa sau datele sunt gresite
                    finalStatus = reason;
                    finalColor = Brushes.OrangeRed;
                    break;
                }

                attempt++;
                int currentAttempt = attempt;
                Dispatcher.UIThread.Post(() =>
                {
                    SetConnectedUi(false);
                    SetStatus($"{reason} Reconectare in {ReconnectDelay.TotalSeconds:0}s (incercarea {currentAttempt})...", Brushes.DarkOrange);
                });

                try
                {
                    await Task.Delay(ReconnectDelay, token);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }

            Dispatcher.UIThread.Post(() => ResetUi(finalStatus, finalColor));
        }

        // o conexiune: trimite abonarea, apoi primeste mesaje si trimite ACK-uri; intoarce motivul inchiderii
        private async Task<string> ListenAsync(MessageBroker.MessageBrokerClient client, string clientId, CancellationToken token, Action onSubscribed)
        {
            using AsyncDuplexStreamingCall<ReceiverFrame, BrokerFrame> call = client.Subscribe(cancellationToken: token);

            var request = new SubscribeRequest { ClientId = clientId };
            request.Topics.AddRange(GetTopics());
            await call.RequestStream.WriteAsync(new ReceiverFrame { Subscribe = request });

            // raspunsul la abonare trebuie sa soseasca in 5 secunde (ca ReadTimeout-ul din Partea 1)
            using (var firstFrameTimeout = CancellationTokenSource.CreateLinkedTokenSource(token))
            {
                firstFrameTimeout.CancelAfter(FirstFrameTimeout);
                try
                {
                    if (!await call.ResponseStream.MoveNext(firstFrameTimeout.Token))
                    {
                        return "Brokerul a inchis conexiunea.";
                    }
                }
                catch (RpcException) when (firstFrameTimeout.IsCancellationRequested && !token.IsCancellationRequested)
                {
                    throw new RpcException(new Status(StatusCode.DeadlineExceeded, "Brokerul nu a raspuns la abonare in 5 secunde."));
                }
            }

            await HandleFrameAsync(call, call.ResponseStream.Current, onSubscribed, token);
            while (await call.ResponseStream.MoveNext(token))
            {
                await HandleFrameAsync(call, call.ResponseStream.Current, onSubscribed, token);
            }

            return "Brokerul a inchis conexiunea.";
        }

        private async Task HandleFrameAsync(AsyncDuplexStreamingCall<ReceiverFrame, BrokerFrame> call, BrokerFrame frame, Action onSubscribed, CancellationToken token)
        {
            switch (frame.FrameCase)
            {
                case BrokerFrame.FrameOneofCase.Reply:
                    onSubscribed();
                    BrokerReply reply = frame.Reply;
                    Dispatcher.UIThread.Post(() => OnSubscribed(reply));
                    break;

                case BrokerFrame.FrameOneofCase.Message:
                    Message message = frame.Message;
                    bool acknowledge = !_skipAcks;
                    _incomingMessages.Enqueue((message, acknowledge));
                    if (acknowledge)
                    {
                        // confirmarea pleaca imediat, fara sa astepte interfata
                        await SendAckAsync(call, message.Id, token);
                    }
                    break;
            }
        }

        private static async Task SendAckAsync(AsyncDuplexStreamingCall<ReceiverFrame, BrokerFrame> call, string messageId, CancellationToken token)
        {
            try
            {
                await call.RequestStream.WriteAsync(new ReceiverFrame { Ack = new Ack { MessageId = messageId } });
            }
            catch (Exception) when (!token.IsCancellationRequested)
            {
                // brokerul a inchis deja apelul; statusul lui real (ex. Aborted la "Sterge toti") vine la sfarsitul raspunsului
                while (await call.ResponseStream.MoveNext(token))
                {
                }

                throw;
            }
        }

        // erorile dupa care nu are sens reconectarea automata ("Sterge toti" trimite Aborted);
        // inainte de prima abonare reusita orice eroare este finala
        private static bool IsFinal(RpcException ex, bool subscribed) =>
            !subscribed || ex.StatusCode is StatusCode.InvalidArgument or StatusCode.FailedPrecondition or StatusCode.Aborted;

        private static string DescribeStop(RpcException ex) => ex.StatusCode switch
        {
            StatusCode.Aborted => $"Deconectat de broker: {BrokerChannelFactory.Describe(ex)}",
            StatusCode.InvalidArgument or StatusCode.AlreadyExists or StatusCode.FailedPrecondition => $"Abonare respinsa: {BrokerChannelFactory.Describe(ex)}",
            _ => $"Eroare de conexiune: {BrokerChannelFactory.Describe(ex)}"
        };

        private void OnSubscribed(BrokerReply reply)
        {
            SetConnectedUi(true);
            ConnectionText.Text = $"Conectat la {_brokerUrl} (gRPC / HTTP2) · brokerul te vede ca {reply.ClientEndpoint}";
            RefreshTopicsText();
            SetStatus($"Conectat ca '{_clientId}'. {reply.Detail}", Brushes.SeaGreen);
        }

        private void FlushMessages()
        {
            if (_incomingMessages.IsEmpty)
            {
                return;
            }

            var entries = new List<ReceivedEntry>();
            while (_incomingMessages.TryDequeue(out (Message Message, bool Acknowledged) item))
            {
                entries.Add(CreateEntry(item.Message, item.Acknowledged));
            }

            // se afiseaza doar ultimele intrari; celelalte ar fi oricum scoase din lista
            foreach (ReceivedEntry entry in entries.Skip(Math.Max(0, entries.Count - MaxFeedEntries)))
            {
                _feed.Insert(0, entry);
            }

            while (_feed.Count > MaxFeedEntries)
            {
                _feed.RemoveAt(_feed.Count - 1);
            }
        }

        private ReceivedEntry CreateEntry(Message message, bool acknowledged)
        {
            var details = new List<string> { FormatTime(message.Timestamp) };
            if (!string.IsNullOrEmpty(message.SenderId))
            {
                details.Add($"de la {message.SenderId}");
            }

            // un mesaj deja primit (reincercare dupa un ACK lipsa sau replay la reconectare) este marcat, nu ascuns
            if (!_receivedIds.Add(message.Id))
            {
                details.Add("duplicat (mesaj deja primit)");
            }

            if (!acknowledged)
            {
                details.Add("fara ACK (test)");
            }

            return new ReceivedEntry
            {
                Topic = message.Topic,
                Payload = message.Payload,
                Time = string.Join(" · ", details)
            };
        }

        // ora publicarii, din timestamp-ul pus de sender; un timestamp invalid nu opreste afisarea mesajului
        private static string FormatTime(Timestamp? timestamp)
        {
            try
            {
                return timestamp?.ToDateTime().ToLocalTime().ToString("HH:mm:ss") ?? "--:--:--";
            }
            catch (InvalidOperationException)
            {
                return "--:--:--";
            }
        }

        private void ResetUi(string status, IBrush color)
        {
            _connection?.Dispose();
            _connection = null;
            _channel?.Dispose();
            _channel = null;
            _client = null;

            ConnectButton.Content = "Conecteaza receiverul";
            ConnectButton.IsEnabled = true;
            BrokerAddressBox.IsEnabled = true;
            ClientIdBox.IsEnabled = true;
            TopicBox.IsEnabled = true;
            SetConnectedUi(false);
            ConnectionText.Text = string.Empty;
            lock (_topicsLock)
            {
                _subscribedTopics.Clear();
            }

            RefreshTopicsText();
            SetStatus(status, color);
        }

        private void SetConnectedUi(bool connected)
        {
            NewTopicBox.IsEnabled = connected;
            AddTopicButton.IsEnabled = connected;
        }

        private string[] GetTopics()
        {
            lock (_topicsLock)
            {
                return _subscribedTopics.ToArray();
            }
        }

        private void RefreshTopicsText()
        {
            string[] topics = GetTopics();
            TopicsText.Text = topics.Length == 0
                ? string.Empty
                : $"Abonat la: {string.Join(", ", topics.OrderBy(t => t, StringComparer.OrdinalIgnoreCase))}";
        }

        // "Curs, PAD" -> ["Curs", "PAD"]; fiecare topic este validat de broker
        private static string[] ParseTopics(string? text) =>
            (text ?? string.Empty).Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        private void SetStatus(string text, IBrush color)
        {
            StatusText.Text = text;
            StatusText.Foreground = color;
            StatusDot.Fill = color;
        }
    }
}
