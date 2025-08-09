namespace Brimborium.Extensions.Logging.LocalFile {
    /// <summary>
    /// Internal Logger - if inner failed.
    /// </summary>
    public class InternalLogger {
        private static InternalLogger? _Instance;

        /// <summary>
        /// Singleton
        /// </summary>
        public static InternalLogger GetInstance()
            => _Instance ??= new InternalLogger();

        private InternalLogger() { }

        /// <summary>
        /// System.Console.Error.WriteLine
        /// </summary>
        /// <param name="error">the thrown exception.</param>
        public void Fail(System.Exception error) {
            if (this.OnFail is { } onFail) {
                try {
                    onFail(error);
                } catch {
                }
            } else {
                if (error is System.AggregateException aggregateException) {
                    aggregateException.Handle(static (error) => true);
                    System.Console.Error.WriteLine(aggregateException.ToString());
                } else {
                    System.Console.Error.WriteLine(error.ToString());
                }
            }
        }

        /// <summary>
        /// Called by <see cref="Fail(System.Exception)"/>
        /// </summary>
        public System.Action<System.Exception>? OnFail { get; set; }
    }
}
