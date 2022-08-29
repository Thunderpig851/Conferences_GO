import json, pika, django, os, sys, time

from pika.exceptions import AMQPConnectionError
from django.core.mail import send_mail

sys.path.append("")
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "presentation_mailer.settings")
django.setup()


def process_approval(ch, method, properties, body):
    data = json.loads(body)
    send_mail(
        "Your presentation has been accepted",
        data["presenter_name"]
        + ", we're happy to tell you that your presentation "
        + data["title"]
        + " has been accepted",
        "admin@conference.com",
        [data["presenter_email"]],
        fail_silently=False,
    )


def process_rejection(ch, method, properties, body):
    data = json.loads(body)
    send_mail(
        "Your presentation has been rejected",
        data["presenter_name"]
        + ", we're sad to tell you that your presentation "
        + data["title"]
        + " has been rejected",
        "admin@conference.com",
        [data["presenter_email"]],
        fail_silently=False,
    )


def consume_message(queue_name, callback):
    channel.queue_declare(queue=queue_name)
    channel.basic_consume(
        queue=queue_name,
        on_message_callback=callback,
        auto_ack=True,
    )


while True:
    try:
        parameters = pika.ConnectionParameters(host="rabbitmq")
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        consume_message("presentation_approvals", process_approval)
        consume_message("presentation_rejections", process_rejection)

        channel.start_consuming()
    except AMQPConnectionError:
        print("Could not connect to RabbitMQ")
        time.sleep(2.0)
