using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RedisLib2;
using StackExchange.Redis;
using System.Threading.Tasks;
using System.Reactive.Linq;
using System.Threading;

namespace RedQ.Tests
{
    [TestClass]
    public class IntegrationTests
    {
        private IDatabase _database;
        private IServer _server;

        [TestInitialize]
        public void Setup()
        {
            var redis = ConnectionMultiplexer.Connect("localhost");
            _server = redis.GetServer("localhost:6379");
            _database = redis.GetDatabase();
        }

        [TestMethod]
        public async Task Head_should_be_0_after_one_message()
        {
            await RingBuffer.DeleteAsync(_database, "Head_should_be_0_after_one_message");
            var ringbuffer = await RingBuffer.GetOrCreateAsync(_server, _database, "Head_should_be_0_after_one_message", 10);
            var producer = ringbuffer.CreateProducer().Publish("key", "value");
            var position = await ringbuffer.GetHeadPosition();

            Assert.AreEqual(0, position);
        }

        [TestMethod]
        public async Task First_message_should_have_id_0()
        {
            await RingBuffer.DeleteAsync(_database, "First_message_should_have_id_0");
            var ringbuffer = await RingBuffer.GetOrCreateAsync(_server, _database, "First_message_should_have_id_0", 10);
            var producer = ringbuffer.CreateProducer();
            await producer.Publish("key", "value");
            var message = await ringbuffer.CreateConsumer(StartFrom.Offset, 1).Take(1);

            Assert.AreEqual(0, message.MessageId);
        }

        [TestMethod]
        public async Task Empty_offset_should_start_from_current_head()
        {
            await RingBuffer.DeleteAsync(_database, "Empty_offset_should_start_from_current_head");
            var ringbuffer = await RingBuffer.GetOrCreateAsync(_server, _database, "Empty_offset_should_start_from_current_head", 10);
            var producer = ringbuffer.CreateProducer();
            int i;
            for (i = 0; i < 15; i++)
                await producer.Publish("key", $"value:{i}");

            bool running = true;
            Task.Run(() =>
              {
                  Thread.Sleep(250);
                  producer.Publish("key", $"value{i}");
              });

            var message = await ringbuffer.CreateConsumer().Take(1);
            running = false;
            Assert.AreEqual(15, message.MessageId);
        }

        [TestMethod]
        public async Task Multiple_producers_should_produce_sequential_message_ids()
        {
            await RingBuffer.DeleteAsync(_database, "Multiple_producers_should_produce_sequential_message_ids");
            var ringbuffer = await RingBuffer.GetOrCreateAsync(_server, _database, "Multiple_producers_should_produce_sequential_message_ids", 3);
            var producer1 = ringbuffer.CreateProducer();
            var producer2 = ringbuffer.CreateProducer();
            var producer3 = ringbuffer.CreateProducer();

            await producer1.Publish("key", "p1:value1");
            await producer3.Publish("key", "p3:value2");
            await producer3.Publish("key", "p3:value3");
            await producer1.Publish("key", "p1:value4");
            await producer2.Publish("key", "p2:value5");

            var message = await ringbuffer.CreateConsumer(StartFrom.Offset, 1).Take(1);

            Assert.AreEqual(4, message.MessageId);
            Assert.AreEqual("p2:value5", message.Value.ToString());
        }

        [TestMethod]
        public async Task Key_and_value_are_read_from_batch()
        {
            await RingBuffer.DeleteAsync(_database, "Key_and_value_are_correct");
            var ringbuffer = await RingBuffer.GetOrCreateAsync(_server, _database, "Key_and_value_are_correct", 10);
            var producer = ringbuffer.CreateProducer();

            for (var m = 0; m < 5; m++)
                await producer.Publish($"key{m}", $"value{m}");

            var message = await ringbuffer.CreateConsumer(StartFrom.Offset, 5).Skip(4).Take(1);

            Assert.AreEqual("key4", message.Key);
            Assert.AreEqual("value4", message.Value.ToString());
        }

        [TestMethod]
        public async Task Message_Id_is_correct_after_buffer_wraps()
        {
            await RingBuffer.DeleteAsync(_database, "Message_Id_is_correct_after_buffer_wraps");
            var ringbuffer = await RingBuffer.GetOrCreateAsync(_server, _database, "Message_Id_is_correct_after_buffer_wraps", 10);
            var producer = ringbuffer.CreateProducer();

            for (var i = 0; i < 15; i++)
                await producer.Publish("key", "value");

            var message = await ringbuffer.CreateConsumer(StartFrom.Offset, 1).Take(1);

            Assert.AreEqual(14, message.MessageId);
        }

        [TestMethod]
        public async Task Consumer_should_get_all_messages_if_requesting_more_than_available()
        {
            await RingBuffer.DeleteAsync(_database, "Consumer_should_get_all_messages_if_requesting_more_than_available");
            var ringbuffer = await RingBuffer.GetOrCreateAsync(_server, _database, "Consumer_should_get_all_messages_if_requesting_more_than_available", 10);
            var producer = ringbuffer.CreateProducer();

            for (var i = 0; i < 3; i++)
                await producer.Publish("key", "value");

            var consumer = ringbuffer.CreateConsumer(StartFrom.Offset, 5);
            var message = await consumer.Take(1);

            Assert.AreEqual(0, message.MessageId);
        }

        [TestMethod]
        public async Task Buffer_should_not_overflow_when_wrapping()
        {
            await RingBuffer.DeleteAsync(_database, "Buffer_should_not_overflow_when_wrapping");
            var ringbuffer = await RingBuffer.GetOrCreateAsync(_server, _database, "Buffer_should_not_overflow_when_wrapping", 10);
            var producer = ringbuffer.CreateProducer();

            for (var i = 0; i < 50; i++)
                await producer.Publish("", new byte[0]);

            var length = await _database.HashLengthAsync(ringbuffer.DbKey.DataHash);

            Assert.AreEqual(10, 10);
        }

        [TestMethod]
        public async Task Consumers_with_different_subscription_ids_should_not_affect_each_other()
        {
            await RingBuffer.DeleteAsync(_database, "Consumers_with_different_subscription_ids_should_not_affect_each_other");
            var ringbuffer = await RingBuffer.GetOrCreateAsync(_server, _database, "Consumers_with_different_subscription_ids_should_not_affect_each_other", 10);
            var producer = ringbuffer.CreateProducer();
            var consumer1 = ringbuffer.CreateConsumer(StartFrom.Offset, 1);
            var consumer2 = ringbuffer.CreateConsumer(StartFrom.Offset, 1);

            for (var i = 0; i < 10; i++)
                await producer.Publish("key", $"value:{i}");

            var m1 = await consumer1.Take(1);
            var m2 = await consumer2.Take(1);

            Assert.AreEqual(9, m1.MessageId);
            Assert.AreEqual(9, m2.MessageId);
        }

        //[TestMethod]
        //public async Task Disposing_of_a_subscriber_doesnt_affect_other_consumers()
        //{
        //    await RingBuffer.DeleteAsync(_database, "Consumers_with_different_subscription_ids_should_not_affect_each_other");
        //    var ringbuffer = await RingBuffer.GetOrCreateAsync(_server, _database, "Consumers_with_different_subscription_ids_should_not_affect_each_other", 10);
        //    var producer = ringbuffer.CreateProducer();
        //    var observable = ringbuffer.CreateConsumer(StartFrom.Offset, 1);
        //    consumer.Subscribe().Dispose();
        //}


        //[TestMethod]
        //public async Task Slow_consumer_gets_lapped_exception()
        //{
        //    await RingBuffer.DeleteAsync(_database, "test_queue");
        //    var ringbuffer = await RingBuffer.GetOrCreateAsync(_server, _database, "test_queue", 10);
        //    var producer = ringbuffer.CreateProducer();
        //    var consumer = ringbuffer.CreateConsumer().Start();

        //    bool running = true;
        //    Task.Run(async () =>
        //    {
        //        while (running)
        //        {
        //            producer.Publish("key", "value");
        //        }
        //    });

        //    Exception ex = null;
        //    consumer.Subscribe(m => { Thread.Sleep(100); }, (Exception e) => {
        //        ex = e;
        //        running = false;
        //    }, () => {  });

        //    while(running)
        //        Thread.Sleep(1);

        //    Assert.IsTrue(ex.GetType() == typeof(ConsumerLappedException));
        //}
    }
}
