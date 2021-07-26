import os
import time

from telegram.ext import Updater, CommandHandler, CallbackContext
from telegram import Update, Message

from olx import Application


def edit_message(context: CallbackContext):
    message: Message = context.job.context.user_data['message']
    text = message.text.replace('.', '')
    context.job.context.user_data['count'] = (1 + context.job.context.user_data['count']
                                              % context.job.context.user_data['max_count'])
    message.edit_text(text + '.' * context.job.context.user_data['count'])


def parse(context: CallbackContext):
    filename = f'{context.job.name}.csv'
    app = Application(os.getenv('urls').split(','), filename=filename)
    start_time = time.time()
    app.start()
    with open(filename, 'rb', encoding='utf-8', newline='') as csv_file:
        context.bot.send_document(context.job.name, csv_file.read(),
                                  caption=f'Время сбора: {time.time() - start_time:.2f}')


def start(update: Update, context: CallbackContext):
    chat_id = update.message.chat_id
    for job in context.job_queue.get_jobs_by_name(str(chat_id)):
        job.schedule_removal()
    context.job_queue.run_once(parse, 0, context=chat_id, name=str(chat_id))
    message = update.message.reply_text('Производится сбор...')
    context.user_data['message'] = message
    context.user_data['max_count'] = message.text.count('.')
    context.user_data['count'] = 1
    context.job_queue.run_repeating(edit_message, 0.5, 0, context=context, name=str(chat_id))


def main():
    updater = Updater(os.getenv('tg_token'))
    updater.dispatcher.add_handler(CommandHandler('start', start, pass_job_queue=True))
    updater.start_polling()
    updater.idle()


if __name__ == '__main__':
    main()