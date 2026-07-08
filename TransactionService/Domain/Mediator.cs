namespace Peikon.Transactions.Domain;

/// <summary>
/// Plain-DI mediator (Mediator pattern without MediatR — v13 went
/// commercial/RPL). Handlers are ordinary scoped services; the mediator is a
/// thin resolver, and cross-cutting behaviors are decorators registered
/// around specific handlers.
/// </summary>
public interface ICommand<TResult> { }

public interface ICommandHandler<in TCommand, TResult> where TCommand : ICommand<TResult>
{
    Task<TResult> HandleAsync(TCommand command, CancellationToken ct);
}

public interface IMediator
{
    Task<TResult> SendAsync<TResult>(ICommand<TResult> command, CancellationToken ct);
}

public sealed class Mediator(IServiceProvider services) : IMediator
{
    public Task<TResult> SendAsync<TResult>(ICommand<TResult> command, CancellationToken ct)
    {
        var handlerType = typeof(ICommandHandler<,>).MakeGenericType(command.GetType(), typeof(TResult));
        dynamic handler = services.GetRequiredService(handlerType);
        return handler.HandleAsync((dynamic)command, ct);
    }
}
