using Grpc.Core;
using Grpc.Core.Interceptors;

namespace BrokerApp.Services;

// trece in jurnalul brokerului cererile respinse si transforma exceptiile neprevazute in StatusCode.Internal,
// astfel incat o cerere gresita nu poate opri brokerul
public sealed class BrokerErrorInterceptor : Interceptor
{
    private readonly BrokerServer _broker;

    public BrokerErrorInterceptor(BrokerServer broker) => _broker = broker;

    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request, ServerCallContext context, UnaryServerMethod<TRequest, TResponse> continuation)
    {
        try
        {
            return await continuation(request, context);
        }
        catch (RpcException ex)
        {
            ReportRejected(ex, context);
            throw;
        }
        catch (Exception ex) when (!IsDisconnect(ex, context))
        {
            throw Unexpected(ex, context);
        }
    }

    public override async Task DuplexStreamingServerHandler<TRequest, TResponse>(
        IAsyncStreamReader<TRequest> requestStream, IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context, DuplexStreamingServerMethod<TRequest, TResponse> continuation)
    {
        try
        {
            await continuation(requestStream, responseStream, context);
        }
        catch (RpcException ex)
        {
            ReportRejected(ex, context);
            throw;
        }
        catch (Exception ex) when (!IsDisconnect(ex, context))
        {
            throw Unexpected(ex, context);
        }
    }

    // inchiderile cerute de broker (oprire, "Sterge toti") nu sunt erori
    private void ReportRejected(RpcException ex, ServerCallContext context)
    {
        if (ex.StatusCode is StatusCode.Unavailable or StatusCode.Aborted or StatusCode.Cancelled)
        {
            return;
        }

        _broker.ReportRejectedCall(MethodName(context), BrokerGrpcService.GetEndpoint(context), ex.Status);
    }

    private RpcException Unexpected(Exception ex, ServerCallContext context)
    {
        _broker.ReportUnexpectedError(MethodName(context), ex);
        return new RpcException(new Status(StatusCode.Internal,
            "INTERNAL_ERROR: Eroare interna in broker; cererea a fost respinsa, brokerul ramane activ."));
    }

    // clientul a inchis conexiunea: nu este o eroare a brokerului
    private static bool IsDisconnect(Exception ex, ServerCallContext context) =>
        ex is OperationCanceledException || (ex is IOException && context.CancellationToken.IsCancellationRequested);

    private static string MethodName(ServerCallContext context) => context.Method[(context.Method.LastIndexOf('/') + 1)..];
}
