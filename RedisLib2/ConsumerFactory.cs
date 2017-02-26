using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RedisLib2
{
    public static class ConsumerFactory
    {
        public static IObservable<Message> Create(RingBuffer ringbuffer, ConsumerOptions options, StartFrom? startFrom = null, long? value = null)
        {
            if (startFrom == null)
                startFrom = StartFrom.Head;

            var consumer = new Consumer(ringbuffer, options);
            consumer.StartFrom = startFrom;
            consumer.StartFromValue = value;

            return Observable.Create((IObserver<Message> observer) =>
            {
                consumer.AddObserver(observer);
                return Disposable.Create(() =>
                {
                    consumer.RemoveObserver(observer);
                });
            });
        }
    }
}
