namespace AngularCore.WebApi;

public class Program {
    public static void Main(string[] args) {
        var builder = WebApplication.CreateBuilder(args);

        // Add services to the container.
        // Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
        builder.Services.AddOpenApi();

        var app = builder.Build();

        // Configure the HTTP request pipeline.
        if (app.Environment.IsDevelopment()) {
            app.MapOpenApi();
        }

#if DEBUG
#else
        app.UseHttpsRedirection();
#endif

        app.MapGet("/", () => {
            return "content";
        })
        .WithName("GetWeatherForecast");

        app.Run();
    }
}
