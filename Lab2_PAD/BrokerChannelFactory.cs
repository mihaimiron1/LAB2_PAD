using System.Text.RegularExpressions;
using Grpc.Core;
using Grpc.Net.Client;

namespace MessageHub.Configuration;

// canalul gRPC catre broker, folosit de Sender si Receiver: HTTP/2 fara TLS, o singura conexiune TCP pe care
// se multiplexeaza toate apelurile (Publish, ListTopics, Subscribe, AddTopics)
public static class BrokerChannelFactory
{
    // erorile trimise de broker incep cu un cod, ex. "INVALID_TOPIC: Topicul este obligatoriu."
    private static readonly Regex BrokerErrorPattern = new(@"^[A-Z_]+: ", RegexOptions.Compiled);

    public static GrpcChannel Create(string host, int port)
    {
        var handler = new SocketsHttpHandler
        {
            // un IP gresit din LAN nu mai blocheaza clientul ~20 de secunde
            ConnectTimeout = TimeSpan.FromSeconds(3),
            // ping-uri HTTP/2 cat timp exista un apel activ (stream-ul receiverului): un broker cazut este detectat
            KeepAlivePingDelay = TimeSpan.FromSeconds(15),
            KeepAlivePingTimeout = TimeSpan.FromSeconds(10),
            KeepAlivePingPolicy = HttpKeepAlivePingPolicy.WithActiveRequests,
            // proxy-ul configurat in Windows nu stie HTTP/2 fara TLS
            UseProxy = false,
            // o singura conexiune HTTP/2 per client: un client are cel mult cateva apeluri simultane,
            // mult sub limita de 100 de stream-uri pe conexiune
            EnableMultipleHttp2Connections = false
        };

        return GrpcChannel.ForAddress(GetAddress(host, port), new GrpcChannelOptions
        {
            HttpHandler = handler,
            DisposeHttpClient = true
        });
    }

    public static string GetAddress(string host, int port) => $"http://{host}:{port}";

    // mesaj scurt pentru interfata: motivul dat de broker sau o explicatie pentru erorile de conexiune
    public static string Describe(RpcException ex)
    {
        if (BrokerErrorPattern.IsMatch(ex.Status.Detail))
        {
            return ex.Status.Detail;
        }

        return ex.StatusCode switch
        {
            // conexiunea HTTP/2 inchisa de broker (oprire sau repornire) inainte ca statusul final sa fie citit
            StatusCode.Internal when ex.Status.Detail.Contains("closed the connection", StringComparison.OrdinalIgnoreCase) =>
                "Brokerul a inchis conexiunea (oprit sau repornit).",
            StatusCode.Unavailable => "Brokerul nu raspunde (adresa gresita, broker oprit sau blocat de firewall).",
            StatusCode.DeadlineExceeded => "Brokerul nu a raspuns la timp.",
            StatusCode.Cancelled => "Cererea a fost anulata.",
            _ => $"{ex.StatusCode}: {ex.Status.Detail}"
        };
    }
}
