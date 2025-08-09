namespace Microsoft.Extensions.DependencyInjection {
    using global::System;
    using global::Microsoft.Extensions.DependencyInjection.Extensions;

    /// <summary>
    /// GetService, but lazy - to break circle in the DI.
    /// </summary>
    /// <typeparam name="T">The ServiceType</typeparam>
    public class LazyGetService<T>
        where T : notnull {
        /// <summary>
        /// for testing
        /// </summary>
        /// <param name="value">the service that <see cref="GetService"/> returns.</param>
        public static LazyGetService<T> Create(T value) {
            return new LazyGetService<T>(value);
        }

        private LazyGetService(T value) {
            this._serviceProvider = new NullServiceProvider();
            this._service = value;
            this._serviceResolved = true;
        }

        private readonly IServiceProvider _serviceProvider;
        private bool _serviceResolved;
        private T? _service;

        /// <summary>
        /// Used by the DI.
        /// </summary>
        /// <param name="serviceProvider">The DI.</param>
        [Microsoft.Extensions.DependencyInjection.ActivatorUtilitiesConstructor]
        public LazyGetService(IServiceProvider serviceProvider) {
            this._serviceProvider = serviceProvider;
        }

        /// <summary>
        /// Resolves the service once.
        /// </summary>
        /// <returns></returns>
        public T? GetService() {
            if (!this._serviceResolved) {
                lock (this) {
                    if (!this._serviceResolved) {
                        this._service = this._serviceProvider.GetService<T>();
                        this._serviceResolved = true;
                    }
                }
            }
            return this._service!;
        }

        /// <summary>
        /// for testing
        /// </summary>
        /// <param name="value">set a value</param>
        public void SetService(T value) {
            lock (this) {
                this._service = value;
                this._serviceResolved = true;
            }
        }

        // for testing
        private class NullServiceProvider : IServiceProvider {
            public object? GetService(Type serviceType) => null;
        }
    }


    /// <summary>
    /// GetRequiredService, but lazy - to break circle in the DI.
    /// </summary>
    /// <typeparam name="T">The ServiceType</typeparam>
    public class LazyGetRequiredService<T>
        where T : notnull {
        /// <summary>
        /// for testing
        /// </summary>
        /// <param name="value">the service that <see cref="GetService"/> returns.</param>

/* Unmerged change from project 'ConsoleAppCore'
Before:
        public static LazyResolveRequiredService<T> Create(T value) {
            return new LazyResolveRequiredService<T>(value);
After:
        public static LazyGetRequiredService<T> Create(T value) {
            return new LazyResolveRequiredService<T>(value);
*/
        public static LazyGetRequiredService<T> Create(T value) {
            return new LazyGetRequiredService<T>(value);
        }

        private LazyGetRequiredService(T value) {
            this._serviceProvider = new NullServiceProvider();
            this._service = value;
            this._serviceResolved = true;
        }

        private readonly IServiceProvider _serviceProvider;
        private bool _serviceResolved;
        private T? _service;

        /// <summary>
        /// Used by the DI.
        /// </summary>
        /// <param name="serviceProvider">The DI.</param>
        [Microsoft.Extensions.DependencyInjection.ActivatorUtilitiesConstructor]
        public LazyGetRequiredService(IServiceProvider serviceProvider) {
            this._serviceProvider = serviceProvider;
        }

        /// <summary>
        /// Resolves the service once.
        /// </summary>
        /// <returns></returns>
        public T GetService() {
            if (!this._serviceResolved) {
                lock (this) {
                    if (!this._serviceResolved) {
                        this._service = this._serviceProvider.GetRequiredService<T>();
                        this._serviceResolved = true;
                    }
                }
            }
            return this._service!;
        }

        /// <summary>
        /// for testing
        /// </summary>
        /// <param name="value">set a value</param>
        public void SetService(T value) {
            lock (this) {
                this._service = value;
                this._serviceResolved = true;
            }
        }

        // for testing
        private class NullServiceProvider : IServiceProvider {
            public object? GetService(Type serviceType) => null;
        }
    }

    public static class LazyGetServiceExtension {
        /// <summary>
        /// Add <see cref="LazyGetService{T}"/> and <see cref="LazyGetRequiredService{T}"/> to the services.
        /// </summary>
        /// <param name="services">The service collection.</param>
        /// <returns>fluent this.</returns>
        public static IServiceCollection AddLazyGetService(this IServiceCollection services) {
            services.TryAdd(ServiceDescriptor.Transient(typeof(LazyGetRequiredService<>), typeof(LazyGetRequiredService<>)));
            services.TryAdd(ServiceDescriptor.Transient(typeof(LazyGetService<>), typeof(LazyGetService<>)));
            return services;
        }
    }
}
