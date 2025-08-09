
namespace AngularCore.WebApi.Test;

public class WebApplicationFactory : WebApplicationFactory<AngularCore.WebApi.Program>, IAsyncInitializer
{
    public Task InitializeAsync()
    {
        _ = Server;

        return Task.CompletedTask;
    }
}
