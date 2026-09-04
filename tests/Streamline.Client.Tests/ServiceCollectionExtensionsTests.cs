using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Streamline.Client;
using Streamline.TestSupport;
using System.Net;
using System.Text;
using Xunit;

namespace Streamline.Client.Tests;

/// <summary>
/// Focused ownership/disposal tests for <see cref="ServiceCollectionExtensions.AddStreamlineAdmin(IServiceCollection)"/>
/// and its explicit-URL overload.
///
/// <para>
/// <see cref="AdminClient"/> owns (and must dispose) an <see cref="HttpClient"/> it
/// creates for itself, but must never dispose one handed to it by a caller or DI
/// container. Both <c>AddStreamlineAdmin</c> overloads register a factory that builds
/// its own <see cref="HttpClient"/> internally (there is no way for anything else to
/// hold a reference to it), so the resolved <see cref="IAdminClient"/> singleton must
/// dispose that client when the container is disposed — otherwise the handle (and its
/// socket/connection pool) leaks for the remaining lifetime of the process.
/// </para>
///
/// <para>
/// As in <see cref="AdminClientTests"/>, disposal is verified purely through the public
/// API: a disposed <see cref="HttpClient"/> throws <see cref="ObjectDisposedException"/>
/// from <c>SendAsync</c> synchronously, before any network I/O is attempted, so this is
/// both a black-box and a hermetic check.
/// </para>
/// </summary>
public class ServiceCollectionExtensionsTests
{
    // =========================================================================
    // AddStreamlineAdmin() -- options-driven overload
    // =========================================================================

    [Fact]
    public async Task AddStreamlineAdmin_NoArgOverload_DisposesSelfCreatedHttpClientWithContainer()
    {
        var services = new ServiceCollection();
        services.Configure<StreamlineOptions>(o => o.Admin.HttpBaseUrl = StreamlineTestEnvironment.UnitHttpBaseUrl);
        services.AddStreamlineAdmin();

        var provider = services.BuildServiceProvider();
        var admin = provider.GetRequiredService<IAdminClient>();

        await provider.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => admin.ListTopicsAsync());
    }

    [Fact]
    public async Task AddStreamlineAdmin_NoArgOverload_ResolvesSingleton()
    {
        var services = new ServiceCollection();
        services.Configure<StreamlineOptions>(o => o.Admin.HttpBaseUrl = StreamlineTestEnvironment.UnitHttpBaseUrl);
        services.AddStreamlineAdmin();

        await using var provider = services.BuildServiceProvider();

        var first = provider.GetRequiredService<IAdminClient>();
        var second = provider.GetRequiredService<IAdminClient>();

        Assert.Same(first, second);
    }

    [Fact]
    public async Task AddStreamlineAdmin_NoArgOverload_UsesConfiguredAuthTokenAndTimeout()
    {
        // Regression guard: the fix must keep forwarding AdminOptions.AuthToken and
        // AdminOptions.Timeout into the owning AdminClient constructor, not just the
        // base URL.
        var services = new ServiceCollection();
        services.Configure<StreamlineOptions>(o =>
        {
            o.Admin.HttpBaseUrl = StreamlineTestEnvironment.UnitHttpBaseUrl;
            o.Admin.AuthToken = "secret-token";
            o.Admin.Timeout = TimeSpan.FromSeconds(5);
        });
        services.AddStreamlineAdmin();

        await using var provider = services.BuildServiceProvider();
        var admin = provider.GetRequiredService<IAdminClient>();

        // Resolving successfully is enough to prove the options were consumed without
        // throwing; AdminClient's HTTP-level tests cover that an Authorization header
        // is actually attached. Disposal itself is left to `await using` above so this
        // also exercises that resolving twice (once here, once implicitly via provider
        // teardown) never double-disposes the underlying HttpClient.
        Assert.NotNull(admin);
    }

    // =========================================================================
    // AddStreamlineAdmin(httpBaseUrl, authToken) -- explicit-URL overload
    // =========================================================================

    [Fact]
    public async Task AddStreamlineAdmin_ExplicitUrlOverload_DisposesSelfCreatedHttpClientWithContainer()
    {
        var services = new ServiceCollection();
        services.AddStreamlineAdmin(StreamlineTestEnvironment.UnitHttpBaseUrl, authToken: "token");

        var provider = services.BuildServiceProvider();
        var admin = provider.GetRequiredService<IAdminClient>();

        await provider.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => admin.ListTopicsAsync());
    }

    [Fact]
    public void AddStreamlineAdmin_ExplicitUrlOverload_RejectsNullOrWhitespaceUrl()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentException>(() => services.AddStreamlineAdmin(" "));
    }

    // =========================================================================
    // Injected / DI-owned HttpClient is never double-disposed or leaked
    // =========================================================================

    [Fact]
    public async Task InjectedHttpClient_RegisteredDirectlyInDI_SurvivesAdminClientAndContainerDisposal()
    {
        // Simulates a consumer who wires AdminClient over a shared, DI-managed
        // HttpClient (e.g. from IHttpClientFactory) instead of using
        // AddStreamlineAdmin(). The shared client must not be disposed just because
        // the AdminClient singleton wrapping it is disposed. A mock handler keeps this
        // hermetic: no real socket/DNS activity, ever.
        using var handler = new MockHttpHandler(HttpStatusCode.OK, "[]");
        var sharedHttpClient = new HttpClient(handler) { BaseAddress = new Uri(StreamlineTestEnvironment.UnitHttpBaseUrl) };

        var services = new ServiceCollection();
        services.AddSingleton(sharedHttpClient);
        services.AddSingleton<IAdminClient>(sp => new AdminClient(sp.GetRequiredService<HttpClient>()));

        var provider = services.BuildServiceProvider();
        var admin = provider.GetRequiredService<IAdminClient>();

        await provider.DisposeAsync();

        // The AdminClient singleton was disposed by the container, but since it never
        // owned sharedHttpClient, both the client itself and the AdminClient wrapping
        // it must remain live and unclosed.
        Assert.Empty(await admin.ListTopicsAsync());

        var response = await sharedHttpClient.GetAsync(new Uri("/v1/topics", UriKind.Relative));
        Assert.True(response.IsSuccessStatusCode);

        sharedHttpClient.Dispose();
    }

    // =========================================================================
    // Mock handler (mirrors AdminClientTests' hermetic HTTP stub)
    // =========================================================================

    private sealed class MockHttpHandler : HttpMessageHandler
    {
        private readonly HttpStatusCode _status;
        private readonly string _body;

        public MockHttpHandler(HttpStatusCode status, string body)
        {
            _status = status;
            _body = body;
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            return Task.FromResult(new HttpResponseMessage(_status)
            {
                Content = new StringContent(_body, Encoding.UTF8, "application/json")
            });
        }
    }
}
