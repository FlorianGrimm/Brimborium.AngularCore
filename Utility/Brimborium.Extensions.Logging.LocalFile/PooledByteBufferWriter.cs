namespace System.Text.Json {
    using global::System;
    using global::System.Buffers;
    using global::System.Diagnostics;
    using global::System.Diagnostics.CodeAnalysis;
    using global::System.IO;
    using global::System.Runtime.CompilerServices;
    using global::System.Threading;
    using global::System.Threading.Tasks;

    internal sealed class PooledByteBufferWriter : IBufferWriter<byte>, IDisposable {
        // This class allows two possible configurations: if rentedBuffer is not null then
        // it can be used as an IBufferWriter and holds a buffer that should eventually be
        // returned to the shared pool. If rentedBuffer is null, then the instance is in a
        // cleared/disposed state and it must re-rent a buffer before it can be used again.
        private byte[]? _rentedBuffer;
        private int _index;

        private const int MinimumBufferSize = 256;

        private PooledByteBufferWriter() {
        }

        public PooledByteBufferWriter(int initialCapacity) {
            Debug.Assert(initialCapacity > 0);

            this._rentedBuffer = ArrayPool<byte>.Shared.Rent(initialCapacity);
            this._index = 0;
        }

        public ReadOnlyMemory<byte> WrittenMemory {
            get {
                if (this._rentedBuffer == null) {
                    return (new byte[0]).AsMemory();
                } else {
                    // Debug.Assert(_rentedBuffer != null);
                    Debug.Assert(this._index <= this._rentedBuffer.Length);
                    return this._rentedBuffer.AsMemory(0, this._index);
                }
            }
        }

        public int WrittenCount {
            get {
                if (this._rentedBuffer is null) {
                    return 0;
                } else {
                    Debug.Assert(this._rentedBuffer != null);
                    return this._index;
                }
            }
        }

        public int Capacity {
            get {
                if (this._rentedBuffer is null) {
                    return 0;
                } else {
                    //Debug.Assert(_rentedBuffer != null);
                    return this._rentedBuffer.Length;
                }
            }
        }

        public int FreeCapacity {
            get {
                if (this._rentedBuffer is null) {
                    return 0;
                } else {
                    //Debug.Assert(_rentedBuffer != null);
                    return this._rentedBuffer.Length - this._index;
                }
            }
        }

        public void Clear() {
            this.ClearHelper();
        }

        public void ClearAndReturnBuffers() {
            Debug.Assert(this._rentedBuffer != null);

            this.ClearHelper();
            var toReturn = this._rentedBuffer;
            this._rentedBuffer = null;
            ArrayPool<byte>.Shared.Return(toReturn);
        }

        private void ClearHelper() {
            if (this._rentedBuffer is null) {
                //
            } else {
                // Debug.Assert(_rentedBuffer != null);
                Debug.Assert(this._index <= this._rentedBuffer.Length);

                this._rentedBuffer.AsSpan(0, this._index).Clear();
            }
            this._index = 0;
        }

        // Returns the rented buffer back to the pool
        public void Dispose() {
            if (this._rentedBuffer == null) { return; }

            this.ClearHelper();
            var toReturn = this._rentedBuffer;
            this._rentedBuffer = null;
            ArrayPool<byte>.Shared.Return(toReturn);
        }

        public void InitializeEmptyInstance(int initialCapacity) {
            Debug.Assert(initialCapacity > 0);
            Debug.Assert(this._rentedBuffer is null);

            this._rentedBuffer = ArrayPool<byte>.Shared.Rent(initialCapacity);
            this._index = 0;
        }

        public static PooledByteBufferWriter CreateEmptyInstanceForCaching() => new PooledByteBufferWriter();

        public void Advance(int count) {
            Debug.Assert(this._rentedBuffer != null);
            Debug.Assert(count >= 0);
            Debug.Assert((this._rentedBuffer != null) && (this._index <= this._rentedBuffer.Length - count));

            this._index += count;
        }

        public Memory<byte> GetMemory(int sizeHint = 0) {
            this.CheckAndResizeBuffer(sizeHint);
            return this._rentedBuffer.AsMemory(this._index);
        }

        public Span<byte> GetSpan(int sizeHint = 0) {
            this.CheckAndResizeBuffer(sizeHint);
            return this._rentedBuffer.AsSpan(this._index);
        }

#if NET8_0_OR_GREATER
        internal ValueTask WriteToStreamAsync(Stream destination, CancellationToken cancellationToken) {
            return destination.WriteAsync(this.WrittenMemory, cancellationToken);
        }

        internal void WriteToStream(Stream destination) {
            destination.Write(this.WrittenMemory.Span);
        }
#else
        internal Task WriteToStreamAsync(Stream destination, CancellationToken cancellationToken) {
            Debug.Assert(_rentedBuffer != null);
            return destination.WriteAsync(_rentedBuffer, 0, _index, cancellationToken);
        }

        internal void WriteToStream(Stream destination) {
            Debug.Assert(_rentedBuffer != null);
            destination.Write(_rentedBuffer, 0, _index);
        }
#endif

        private void CheckAndResizeBuffer(int sizeHint) {
            Debug.Assert(sizeHint >= 0);

            if (sizeHint == 0) {
                sizeHint = MinimumBufferSize;
            }

            if (this._rentedBuffer is null) {
                this._rentedBuffer = new byte[sizeHint];
            }
            //Debug.Assert(_rentedBuffer != null);

            var availableSpace = this._rentedBuffer.Length - this._index;

            if (sizeHint > availableSpace) {
                var currentLength = this._rentedBuffer.Length;
                var growBy = Math.Max(sizeHint, currentLength);

                var newSize = currentLength + growBy;

                if ((uint)newSize > int.MaxValue) {
                    newSize = currentLength + sizeHint;
                    if ((uint)newSize > int.MaxValue) {
                        ThrowHelper.ThrowOutOfMemoryException_BufferMaximumSizeExceeded((uint)newSize);
                    }
                }

                var oldBuffer = this._rentedBuffer;

                this._rentedBuffer = ArrayPool<byte>.Shared.Rent(newSize);

                Debug.Assert(oldBuffer.Length >= this._index);
                Debug.Assert(this._rentedBuffer.Length >= this._index);

                var previousBuffer = oldBuffer.AsSpan(0, this._index);
                previousBuffer.CopyTo(this._rentedBuffer);
                previousBuffer.Clear();
                ArrayPool<byte>.Shared.Return(oldBuffer);
            }

            Debug.Assert(this._rentedBuffer.Length - this._index > 0);
            Debug.Assert(this._rentedBuffer.Length - this._index >= sizeHint);
        }
    }


    internal static partial class ThrowHelper {
        [DoesNotReturn]
        [MethodImpl(MethodImplOptions.NoInlining)]
        public static void ThrowOutOfMemoryException_BufferMaximumSizeExceeded(uint capacity) {
            throw new OutOfMemoryException($"BufferMaximumSizeExceeded {capacity}");
        }
    }
}