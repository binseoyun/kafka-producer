#local에서 돌릴 코드가 아니고 서버 배포 후 EC2 서버 상에서 돌릴 코드
from confluent_kafka import Producer
import sys
import time

#접속할 카프카 브로커의 주소 목록
BROKER_LST = 'kafka01:9092,kafka02:9092,kafka03:9092'


class SimpleProducer:

    def __init__(self, topic, duration=None):
        self.topic = topic
        self.duration = duration if duration is not None else 60
        self.conf = {'bootstrap.servers': BROKER_LST}

        self.producer = Producer(self.conf) #produce 할 시작점

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(self, err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))

    def produce(self):
        cnt = 0
        while cnt < self.duration: #정해진 시간 동안 반복
            try:
                # Produce line (without newline)
                self.producer.produce(
                    topic=self.topic,
                    key=str(cnt),
                    value=f'hello world: {cnt}',
                    on_delivery=self.delivery_callback #전송결과가 오면 이 함수 호출
                )

            except BufferError:
                #프로듀서 내부 버퍼가 꽉 찼을 때 예외 처리
                sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                                 len(self.producer))

            # Serve delivery callback queue.
            # NOTE: Since produce() is an asynchronous API this poll() call
            #       will most likely not serve the delivery callback for the
            #       last produce()d message.
            self.producer.poll(0)
            cnt += 1
            time.sleep(1)  # 1초 대기

        # Wait until all messages have been delivered
        sys.stderr.write('%% Waiting for %d deliveries\n' % len(self.producer))
        self.producer.flush() #프로그램이 종료되기 전에 producer에 남아있는 데이터를 broker로


if __name__ == '__main__':
    simple_producer = SimpleProducer(topic='lesson.ch5-1.simple.producer', duration=60)
    simple_producer.produce()