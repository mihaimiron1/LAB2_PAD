using System.Net;
using Google.Protobuf;
using Grpc.Core;
using MessageHub.Contracts;
using Microsoft.AspNetCore.Http;

namespace BrokerApp.Services;

// endpoint-ul de retea al brokerului: identifica senderii si receiverii, valideaza datele
// si deleaga rutarea/stocarea catre BrokerServer; Kestrel trateaza fiecare apel pe un task separat
public sealed class BrokerGrpcService : MessageBroker.MessageBrokerBase
{
    private static readonly TimeSpan SubscribeRequestTimeout = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan WriteDrainTimeout = TimeSpan.FromSeconds(2);
    private static readonly Status InvalidFrameStatus = new(StatusCode.InvalidArgument, "INVALID_SUBSCRIPTION: Cadru Protobuf invalid pe stream-ul Subscribe.");
    private readonly BrokerServer _broker;

    public BrokerGrpcService(BrokerServer broker) => _broker = broker;

    public override Task<BrokerReply> Publish(PublishRequest request, ServerCallContext context)
    {
        Message message = MessageValidator.ValidatePublish(request);
        return Task.FromResult(_broker.Publish(message, GetEndpoint(context)));
    }

    public override Task<TopicList> ListTopics(ListTopicsRequest request, ServerCallContext context)
    {
        var reply = new TopicList { ClientEndpoint = GetEndpoint(context) };
        reply.Topics.AddRange(_broker.GetActiveTopics());
        return Task.FromResult(reply);
    }

    public override Task<BrokerReply> AddTopics(AddTopicsRequest request, ServerCallContext context)
    {
        string clientId = MessageValidator.ValidateClientId(request.ClientId);
        HashSet<string> topics = MessageValidator.ValidateTopics(request.Topics);
        return Task.FromResult(_broker.AddTopics(clientId, topics, GetEndpoint(context)));
    }

    // canalul de lunga durata al receiverului: cererea de abonare, apoi mesaje catre receiver si ACK-uri inapoi
    public override async Task Subscribe(IAsyncStreamReader<ReceiverFrame> requestStream, IServerStreamWriter<BrokerFrame> responseStream, ServerCallContext context)
    {
        SubscribeRequest request;
        try
        {
            request = await ReadSubscribeRequestAsync(requestStream, context.CancellationToken);
        }
        catch (InvalidProtocolBufferException)
        {
            // date corupte (este si un IOException, deci trebuie prins inaintea deconectarilor)
            throw new RpcException(InvalidFrameStatus);
        }
        catch (Exception ex) when (ex is OperationCanceledException or IOException or InvalidOperationException)
        {
            // receiverul a inchis conexiunea inainte de cererea de abonare
            return;
        }

        string clientId = MessageValidator.ValidateClientId(request.ClientId);
        HashSet<string> topics = MessageValidator.ValidateTopics(request.Topics);
        string endpoint = GetEndpoint(context);

        SubscriberSession session = await _broker.OpenSessionAsync(clientId, topics, endpoint, responseStream, context.CancellationToken);
        bool drained;
        try
        {
            // SUBSCRIBED este primul cadru; livrarea porneste separat, ca bucla de mai jos sa poata citi ACK-urile
            await session.SendReplyAsync(BrokerServer.CreateSubscribedReply(topics, endpoint));
            _broker.TriggerDelivery(clientId);

            while (await requestStream.MoveNext(session.Token))
            {
                ReceiverFrame frame = requestStream.Current;
                if (frame.FrameCase == ReceiverFrame.FrameOneofCase.Ack)
                {
                    _broker.Acknowledge(session, frame.Ack.MessageId);
                }
            }
        }
        catch (InvalidProtocolBufferException)
        {
            // un cadru corupt incheie apelul cu InvalidArgument (trecut in jurnal de interceptor)
            session.Close(InvalidFrameStatus);
        }
        catch (Exception ex) when (ex is OperationCanceledException or IOException or InvalidOperationException)
        {
            // receiverul s-a deconectat sau brokerul a inchis sesiunea
        }
        finally
        {
            try
            {
                _broker.CloseSession(session);
                drained = await session.DrainWritesAsync(WriteDrainTimeout);
                if (!drained)
                {
                    // o scriere blocata (receiver care nu mai citeste): apelul se inchide fara status
                    context.GetHttpContext().Abort();
                }
            }
            finally
            {
                _broker.EndSubscribeCall();
            }
        }

        if (drained && session.CloseStatus is Status status)
        {
            throw new RpcException(status);
        }
    }

    // adresa IP:port a clientului (fiecare terminal are propriul port local, alocat de sistemul de operare)
    internal static string GetEndpoint(ServerCallContext context)
    {
        ConnectionInfo connection = context.GetHttpContext().Connection;
        IPAddress? address = connection.RemoteIpAddress;
        if (address is null)
        {
            return context.Peer;
        }

        if (address.IsIPv4MappedToIPv6)
        {
            address = address.MapToIPv4();
        }

        return new IPEndPoint(address, connection.RemotePort).ToString();
    }

    private static async Task<SubscribeRequest> ReadSubscribeRequestAsync(IAsyncStreamReader<ReceiverFrame> requestStream, CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(SubscribeRequestTimeout);
        try
        {
            if (!await requestStream.MoveNext(timeout.Token))
            {
                throw new RpcException(new Status(StatusCode.FailedPrecondition,
                    "INVALID_SUBSCRIPTION: Stream-ul s-a inchis inainte de cererea de abonare."));
            }
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new RpcException(new Status(StatusCode.DeadlineExceeded,
                "INVALID_SUBSCRIPTION: Cererea de abonare nu a sosit in 5 secunde."));
        }

        ReceiverFrame first = requestStream.Current;
        if (first.FrameCase != ReceiverFrame.FrameOneofCase.Subscribe)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition,
                "INVALID_SUBSCRIPTION: Primul cadru trimis pe Subscribe trebuie sa fie cererea de abonare."));
        }

        return first.Subscribe;
    }
}
