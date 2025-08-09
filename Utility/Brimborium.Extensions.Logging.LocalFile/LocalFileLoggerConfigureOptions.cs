namespace Brimborium.Extensions.Logging.LocalFile {
    using global::Microsoft.Extensions.Configuration;
    using global::Microsoft.Extensions.Options;
    using global::System;

    internal sealed class LocalFileLoggerConfigureOptions : IConfigureOptions<LocalFileLoggerOptions> {
        private readonly IConfiguration _Configuration;
        /*
#if LocalFileIHostApplicationLifetime
        private readonly Microsoft.Extensions.Hosting.IHostEnvironment _HostEnvironment;
#endif
        */
        public LocalFileLoggerConfigureOptions(
            /*
#if LocalFileIHostApplicationLifetime
            Microsoft.Extensions.Hosting.IHostEnvironment hostEnvironment,
#endif
            */
            IConfiguration configuration
            ) {

            this._Configuration = configuration;

            //_HostEnvironment = hostEnvironment;
        }

        public void Configure(LocalFileLoggerOptions options) {
            IConfigurationSection? configurationSection;
            if (this._Configuration is IConfigurationRoot configurationRoot) {
                configurationSection = configurationRoot.GetSection("Logging:LocalFile");
            } else if (this._Configuration is IConfigurationSection section) {
                configurationSection = section;
            } else {
                configurationSection = default;
            }
            if (configurationSection is { }
                && configurationSection.Exists()) {
                options.IsEnabled = TextToBoolean(configurationSection.GetSection("IsEnabled")?.Value, true);

                options.FileSizeLimit = TextToInt(
                    configurationSection.GetSection("FileSizeLimit")?.Value,
                    null,
                    (value) => ((value.HasValue) ? value.Value * 1024 * 1024 : null)
                    );
                options.RetainedFileCountLimit = TextToInt(
                    configurationSection.GetSection("FileRetainedFileCountLimit")?.Value,
                    31,
                    (value) => ((value.HasValue) ? value.Value : 10)
                    );
                options.FlushPeriod = TextToTimeSpan(
                    configurationSection.GetSection("FlushPeriod")?.Value
                    ).GetValueOrDefault(
                        TimeSpan.FromSeconds(1)
                    );
                options.IncludeScopes = TextToBoolean(configurationSection.GetSection("IncludeScopes")?.Value);
                options.TimestampFormat = configurationSection.GetSection("TimestampFormat")?.Value;
                options.UseUtcTimestamp = TextToBoolean(configurationSection.GetSection("UseUtcTimestamp")?.Value);
                options.IncludeEventId = TextToBoolean(configurationSection.GetSection("IncludeEventId")?.Value);
                options.UseJSONFormat = TextToBoolean(configurationSection.GetSection("UseJSONFormat")?.Value);

                string? baseDirectory = configurationSection.GetSection("BaseDirectory").Value;
                if (string.IsNullOrEmpty(baseDirectory)) { baseDirectory = options.BaseDirectory; }
                
                if (baseDirectory is { Length: > 0 }
#if NET8_0_OR_GREATER
                    && baseDirectory.Contains('%')
#else
                    && baseDirectory.Contains("%")
#endif
                    ) {
                    baseDirectory = System.Environment.ExpandEnvironmentVariables(baseDirectory);
                    options.BaseDirectory = baseDirectory;
                }


                string? logDirectory = configurationSection.GetSection("Directory").Value;
                if (string.IsNullOrEmpty(logDirectory)) { logDirectory = configurationSection.GetSection("LogDirectory").Value; }
                if (string.IsNullOrEmpty(logDirectory)) { logDirectory = options.LogDirectory; }

                if (logDirectory is { Length: > 0 }
#if NET8_0_OR_GREATER
                    && logDirectory.Contains('%')
#else
                    && logDirectory.Contains("%")
#endif
                    ) {
                    logDirectory = System.Environment.ExpandEnvironmentVariables(logDirectory);
                }
                options.LogDirectory = logDirectory;
            }
            {
                var baseDirectory = options.BaseDirectory;
                var logDirectory = options.LogDirectory;
                if (logDirectory is { Length: > 0 }
                    && System.IO.Path.IsPathRooted(logDirectory)) {
                    options.IsEnabled = true;
                } else if (baseDirectory is { Length: > 0 }
                    && logDirectory is { Length: > 0 }) {
                    var fullDirectory = System.IO.Path.Combine(baseDirectory, logDirectory);
                    options.IsEnabled = System.IO.Path.IsPathRooted(fullDirectory);
                    options.LogDirectory = fullDirectory;

                }
            }
        }

        private static bool TextToBoolean(string? text, bool defaultValue = false)
            => string.IsNullOrEmpty(text) || !bool.TryParse(text, out var result)
                ? defaultValue
                : result;

        private static TimeSpan? TextToTimeSpan(string? text, TimeSpan? defaultValue = default, Func<TimeSpan?, TimeSpan?>? convert = default)
            => string.IsNullOrEmpty(text) || !TimeSpan.TryParse(text, out var result)
                ? convert is null ? defaultValue : convert(defaultValue)
                : convert is null ? result : convert(result);

        private static int? TextToInt(string? text, int? defaultValue = default, Func<int?, int?>? convert = default)
            => string.IsNullOrEmpty(text) || !int.TryParse(text, out var result)
                ? convert is null ? defaultValue : convert(defaultValue)
                : convert is null ? result : convert(result);
    }
}
