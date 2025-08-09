namespace Brimborium.Extensions.Logging.LocalFile {
    internal sealed class NullScope : System.IDisposable {
        internal static NullScope Instance { get; } = new NullScope();

        private NullScope() {}

        public void Dispose() {}
    }
}