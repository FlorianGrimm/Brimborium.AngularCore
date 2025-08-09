namespace Brimborium.Extensions.Logging.LocalFile {
    [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
    internal readonly struct LogMessage {
        public LogMessage(System.DateTimeOffset timestamp, string message) {
            this.Timestamp = timestamp;
            this.Message = message;
        }

        public System.DateTimeOffset Timestamp { get; }
        public string Message { get; }
    }
}
