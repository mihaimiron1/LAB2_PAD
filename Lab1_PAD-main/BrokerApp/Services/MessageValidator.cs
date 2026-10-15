using System.Text.RegularExpressions;
using Grpc.Core;
using MessageHub.Contracts;

namespace BrokerApp.Services;

// validarea datelor primite de la sender si receiver; orice abatere este respinsa cu InvalidArgument,
// iar codul din Partea 1 (INVALID_MESSAGE, INVALID_TOPIC, INVALID_SUBSCRIPTION) ramane la inceputul mesajului
public static class MessageValidator
{
    public const int MaxTopicLength = 64;
    public const int MaxTopicsPerRequest = 20;
    public const int MaxPayloadLength = 4096;

    // limitele lui google.protobuf.Timestamp (0001-01-01 .. 9999-12-31); in afara lor receiverii nu pot afisa ora
    private const long MinTimestampSeconds = -62_135_596_800;
    private const long MaxTimestampSeconds = 253_402_300_799;

    // numele logice (ClientId, SenderId)
    private static readonly Regex NamePattern = new(@"^[A-Za-z0-9_-]{1,32}$", RegexOptions.Compiled);

    public static Message ValidatePublish(PublishRequest request)
    {
        Message? message = request.Message;
        if (message is null)
        {
            throw Invalid("INVALID_MESSAGE", "Cererea Publish trebuie sa contina un mesaj.");
        }

        if (!Guid.TryParse(message.Id, out _))
        {
            throw Invalid("INVALID_MESSAGE", "Id-ul mesajului trebuie sa fie un GUID valid.");
        }

        ValidateName(message.SenderId, "INVALID_MESSAGE", "SenderId");
        ValidateTopic(message.Topic);

        if (string.IsNullOrWhiteSpace(message.Payload))
        {
            throw Invalid("INVALID_MESSAGE", "Mesajul (payload) nu poate fi gol.");
        }

        if (message.Payload.Length > MaxPayloadLength)
        {
            throw Invalid("INVALID_MESSAGE", $"Mesajul depaseste {MaxPayloadLength} de caractere.");
        }

        if (message.Timestamp is null)
        {
            throw Invalid("INVALID_MESSAGE", "Timestamp-ul mesajului este obligatoriu.");
        }

        if (message.Timestamp.Seconds is < MinTimestampSeconds or > MaxTimestampSeconds ||
            message.Timestamp.Nanos is < 0 or > 999_999_999)
        {
            throw Invalid("INVALID_MESSAGE", "Timestamp-ul mesajului este in afara intervalului valid.");
        }

        return message;
    }

    public static string ValidateClientId(string clientId) => ValidateName(clientId, "INVALID_SUBSCRIPTION", "ClientId");

    // topicurile unei cereri Subscribe/AddTopics: spatiile de la capete se elimina, fara duplicate, cel putin unul
    public static HashSet<string> ValidateTopics(IEnumerable<string> rawTopics)
    {
        var topics = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (string rawTopic in rawTopics)
        {
            string topic = rawTopic.Trim();
            if (topic.Length == 0)
            {
                continue;
            }

            ValidateTopic(topic);
            topics.Add(topic);
        }

        if (topics.Count == 0)
        {
            throw Invalid("INVALID_TOPIC", "Cel putin un topic valid este obligatoriu.");
        }

        if (topics.Count > MaxTopicsPerRequest)
        {
            throw Invalid("INVALID_TOPIC", $"O cerere poate contine maxim {MaxTopicsPerRequest} topicuri.");
        }

        return topics;
    }

    // numele logice: 1-32 caractere, litere, cifre, '_' sau '-'
    private static string ValidateName(string value, string code, string field)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw Invalid(code, $"{field} este obligatoriu.");
        }

        if (!NamePattern.IsMatch(value))
        {
            throw Invalid(code, $"{field} poate avea 1-32 caractere: litere, cifre, '_' sau '-'.");
        }

        return value;
    }

    // topicul este tipul mesajului si cheia de rutare
    private static void ValidateTopic(string topic)
    {
        if (string.IsNullOrWhiteSpace(topic))
        {
            throw Invalid("INVALID_TOPIC", "Topicul este obligatoriu.");
        }

        if (topic.Length > MaxTopicLength)
        {
            throw Invalid("INVALID_TOPIC", $"Topicul poate avea maxim {MaxTopicLength} de caractere.");
        }

        if (topic != topic.Trim())
        {
            throw Invalid("INVALID_TOPIC", "Topicul nu poate incepe sau se termina cu spatii.");
        }

        if (topic.Contains(',') || topic.Any(char.IsControl))
        {
            throw Invalid("INVALID_TOPIC", "Topicul nu poate contine virgule (separa mai multe topicuri) sau caractere de control.");
        }
    }

    private static RpcException Invalid(string code, string detail) =>
        new(new Status(StatusCode.InvalidArgument, $"{code}: {detail}"));
}
