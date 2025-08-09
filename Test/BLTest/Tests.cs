using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace AngularCore;
public class Tests {
    [Test]
    public async Task Test01() {
        int i = 0;
        await Assert.That(i).IsEqualTo(0);
    }
}