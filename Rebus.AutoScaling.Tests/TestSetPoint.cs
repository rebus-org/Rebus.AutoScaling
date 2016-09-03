using NUnit.Framework;

namespace Rebus.AutoScaling.Tests
{
    [TestFixture]
    public class TestSetPoint
    {
        [Test]
        public void InitializesToZero()
        {
            var setPoint = new SetPoint(0, 0.001m);

            Assert.That(setPoint.Target, Is.EqualTo(0));
            Assert.That(setPoint.Value, Is.EqualTo(0));
        }
    }
}