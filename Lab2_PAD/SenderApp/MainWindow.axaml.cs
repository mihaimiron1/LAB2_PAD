using System;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using Avalonia.Controls;
using Avalonia.Interactivity;
using Avalonia.Threading;
using Grpc.Core;
using Grpc.Net.Client;
using MessageHub.Configuration;
using MessageHub.Contracts;
using Timestamp = Google.Protobuf.WellKnownTypes.Timestamp;

namespace SenderApp
{
    public class SentEntry
    {
        public string Topic { get; set; } = string.Empty;
        public string Payload { get; set; } = string.Empty;
        public string Time { get; set; } = string.Empty;
        public string StatusIcon { get; set; } = string.Empty;
    }

    public partial class MainWindow : Window
    {
        private readonly ObservableCollection<SentEntry> _history = new();
        private readonly ObservableCollection<string> _topics = new();
        private readonly DispatcherTimer _topicRefreshTimer;
        private bool _isRefreshingTopics;

        // un singur canal gRPC (o conexiune HTTP/2) per terminal; toate accesarile sunt pe firul UI
        private GrpcChannel? _channel;
        private MessageBroker.MessageBrokerClient? _client;
        private string _channelAddress = string.Empty;
        private string _channelUrl = string.Empty;
        private string _brokerAddress = $"{EnvironmentSettings.BrokerHost}:{EnvironmentSettings.BrokerPort}";

        public MainWindow()
        {
            InitializeComponent();
            BrokerAddressBox.Text = _brokerAddress;
            HistoryList.ItemsSource = _history;
            TopicDropdown.ItemsSource = _topics;
            _topicRefreshTimer = new DispatcherTimer { Interval = TimeSpan.FromSeconds(2) };
            _topicRefreshTimer.Tick += async (_, _) => await RefreshTopicsAsync();
            _topicRefreshTimer.Start();
            Opened += async (_, _) => await RefreshTopicsAsync();
            Closed += (_, _) =>
            {
                _topicRefreshTimer.Stop();
                _channel?.Dispose();
            };
        }

        // brokerul poate rula pe alt calculator: adresa se poate schimba din interfata, iar canalul se redeschide
        private void OnBrokerAddressChanged(object? sender, TextChangedEventArgs e)
        {
            if (EnvironmentSettings.TryParseBrokerAddress(BrokerAddressBox.Text, out string host, out int port))
            {
                _brokerAddress = $"{host}:{port}";
            }
        }

        private void OnTopicSelectionChanged(object? sender, SelectionChangedEventArgs e)
        {
            if (TopicDropdown.SelectedItem is string topic && !string.IsNullOrWhiteSpace(topic))
            {
                TopicBox.Text = topic;
            }
        }

        private async void OnSendClick(object? sender, RoutedEventArgs e)
        {
            string senderId = SenderIdBox.Text?.Trim() ?? string.Empty;
            string topic = TopicBox.Text?.Trim() ?? string.Empty;
            string payload = MessageBox.Text?.Trim() ?? string.Empty;

            if (!EnvironmentSettings.TryParseBrokerAddress(BrokerAddressBox.Text, out _, out _))
            {
                StatusText.Text = "Adresa brokerului este invalida (ex: 192.168.1.10:5000).";
                StatusText.Foreground = Avalonia.Media.Brushes.OrangeRed;
                return;
            }

            // id-ul senderului, topicul si mesajul sunt validate de broker; motivul unei respingeri apare in status
            SendButton.IsEnabled = false;
            StatusText.Text = "Se trimite...";
            StatusText.Foreground = Avalonia.Media.Brushes.Gray;

            (bool success, string detail) = await SendMessageAsync(senderId, topic, payload);

            _history.Insert(0, new SentEntry
            {
                Topic = topic,
                Payload = payload,
                Time = DateTime.Now.ToString("HH:mm:ss"),
                StatusIcon = success ? "✅" : "❌"
            });

            StatusText.Text = detail;
            StatusText.Foreground = success ? Avalonia.Media.Brushes.SeaGreen : Avalonia.Media.Brushes.OrangeRed;

            if (success)
            {
                MessageBox.Text = string.Empty;
            }

            await RefreshTopicsAsync();
            SendButton.IsEnabled = true;
        }

        private async Task RefreshTopicsAsync()
        {
            if (_isRefreshingTopics)
            {
                return;
            }

            _isRefreshingTopics = true;
            try
            {
                string[] topics = await GetActiveTopicsAsync();
                TopicDropdown.SelectedItem = null;
                _topics.Clear();
                foreach (string topic in topics)
                {
                    _topics.Add(topic);
                }

                if (_topics.Count == 0)
                {
                    TopicDropdown.PlaceholderText = "(fara topice)";
                }
                else
                {
                    TopicDropdown.PlaceholderText = "Alege topic";
                }
            }
            finally
            {
                _isRefreshingTopics = false;
            }
        }

        private async Task<(bool Success, string Detail)> SendMessageAsync(string senderId, string topic, string payload)
        {
            try
            {
                // senderul serializeaza mesajul (Protobuf, prin codul generat din messagehub.proto)
                var request = new PublishRequest
                {
                    Message = new Message
                    {
                        Id = Guid.NewGuid().ToString(),
                        SenderId = senderId,
                        Topic = topic,
                        Payload = payload,
                        Timestamp = Timestamp.FromDateTime(DateTime.UtcNow)
                    }
                };

                BrokerReply reply = await GetClient().PublishAsync(request, deadline: DateTime.UtcNow.AddSeconds(5));
                ShowConnected(reply.ClientEndpoint);
                return (true, reply.Detail);
            }
            catch (RpcException ex)
            {
                return (false, BrokerChannelFactory.Describe(ex));
            }
            catch (Exception ex)
            {
                return (false, $"Eroare de conexiune la Broker: {ex.Message}");
            }
        }

        private async Task<string[]> GetActiveTopicsAsync()
        {
            try
            {
                TopicList reply = await GetClient().ListTopicsAsync(new ListTopicsRequest(), deadline: DateTime.UtcNow.AddSeconds(2));
                ShowConnected(reply.ClientEndpoint);
                return reply.Topics
                    .Where(topic => !string.IsNullOrWhiteSpace(topic))
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToArray();
            }
            catch (Exception)
            {
                ConnectionText.Text = "Neconectat la broker";
                return Array.Empty<string>();
            }
        }

        // canalul se recreeaza doar cand se schimba adresa brokerului; apelurile se multiplexeaza pe aceeasi conexiune
        private MessageBroker.MessageBrokerClient GetClient()
        {
            if (_client is not null && _brokerAddress == _channelAddress)
            {
                return _client;
            }

            RetireChannel();
            EnvironmentSettings.TryParseBrokerAddress(_brokerAddress, out string host, out int port);
            _channel = BrokerChannelFactory.Create(host, port);
            _client = new MessageBroker.MessageBrokerClient(_channel);
            _channelAddress = _brokerAddress;
            _channelUrl = BrokerChannelFactory.GetAddress(host, port);
            return _client;
        }

        // brokerul raporteaza adresa IP:port de pe care vede acest terminal (portul local este alocat de sistemul de operare)
        private void ShowConnected(string clientEndpoint)
        {
            ConnectionText.Text = $"Conectat la {_channelUrl} (gRPC / HTTP2) · brokerul te vede ca {clientEndpoint}";
        }

        // apelurile aflate in curs pe canalul vechi (ex. un Publish) se termina inainte ca el sa fie inchis
        private void RetireChannel()
        {
            GrpcChannel? oldChannel = _channel;
            _channel = null;
            _client = null;
            _channelAddress = string.Empty;
            if (oldChannel is not null)
            {
                _ = Task.Delay(TimeSpan.FromSeconds(10)).ContinueWith(_ => oldChannel.Dispose(), TaskScheduler.Default);
            }
        }
    }
}
