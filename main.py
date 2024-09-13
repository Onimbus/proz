import telebot
from logic import *
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from telebot.types import ReplyKeyboardMarkup, KeyboardButton
import requests

bot = telebot.TeleBot(TOKEN)

def get_cat():
    url = 'https://api.thecatapi.com/v1/images/search'
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()[0]['url']

def gen_answer_markup(id):
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton('Ответить на вопрос', callback_data=f'answer_{id}'),
                   InlineKeyboardButton("Выбрать другой вопрос", callback_data='another'))
    return markup

def gen_questions_markup(answers):
    markup = InlineKeyboardMarkup(row_width=4)
    for i in range(len(QUESTIONS_INFO)):
        markup.row(InlineKeyboardButton(f'LEVEL {i+1}', callback_data=f' '))
        buttons = []
        for j in QUESTIONS_INFO[i]['list']:
            if j in answers:
                key = manager.get_key_by_id(j)
                buttons.append(InlineKeyboardButton(f"🟩{key}🟩", callback_data=f' '))
            else:
                buttons.append(InlineKeyboardButton(f'🟥{j}🟥', callback_data=f'{j}'))
        if len(buttons)>4:
            buttons = [buttons[i:i + 4] for i in range(0, len(buttons), 4)]
            for buttons_ in buttons:
                markup.row(*buttons_)
        else:
            markup.row(*buttons)
    return markup


def gen_teams_markup(teams):
    markup = InlineKeyboardMarkup(row_width=1)
    for team in teams:
        markup.add(InlineKeyboardButton(team[1], callback_data=f'team_{team[0]}'))
    return markup


@bot.callback_query_handler(func=lambda call: True)
def callback_query(call):
    user_id = call.message.chat.id

    if call.data.isdigit():
        question_id = call.data
        if manager.check_access(int(question_id), user_id):
            manager.update_question_id(user_id, question_id)
            send_question(bot, call.message, user_id)
        else:
            bot.send_message(user_id, 'У тебя пока нет доступа к этому уровню ( Для начала нужно ответить на все вопросы этого уровня или собрать слово из ключей!) ')
    
    if  call.data == 'another':
        get_questions_handler(call.message)

    if call.data.startswith('answer'):
        question_id = int(call.data[7:])
        manager.update_question_id(user_id, question_id)
        id, question, answer = manager.get_question(user_id)
        bot.send_message(user_id, 'Введи ответ:')
        bot.register_next_step_handler(call.message, next_step, id = id, answer=answer)

    if call.data.startswith('team'):
        team_id = int(call.data[5:])
        manager.insert_user(call.message.chat.id, team_id)
        register_step4(call.message)

def gen_rating_markup(rating):
    markup = InlineKeyboardMarkup(row_width=3)
    emogi = ['👑👑👑', '👑👑', '👑', '4️⃣', '5️⃣', '6️⃣', '7️⃣', '8️⃣', '9️⃣', '🔟']

    for idx, row in enumerate(rating, start=1):
        name, score = row
        markup.add(InlineKeyboardButton(emogi[idx-1], callback_data=' '),
                   InlineKeyboardButton(name, callback_data=' '),
                   InlineKeyboardButton(str(score), callback_data=' ')
                   )
    return markup

def send_question(bot, message, user_id):
    id, question, answer = manager.get_question(user_id)
    bot.send_message(user_id, question)
    bot.register_next_step_handler(message, next_step, id = id, answer=answer)


def next_step(message, id, answer):
    user_id = message.chat.id

    if message.text == answer:
        if manager.check_answer(user_id):
            bot.send_message(user_id, 'Ты уже отвечал на этот вопрос!')
        else:
            bot.send_message(user_id, "Отлично! Ответ верный!")
            key = manager.get_key_by_id(manager.get_question(user_id)[0])
            bot.send_message(user_id, f"🎉🎉 Получи ключик {key} 🎉🎉") 

            if manager.add_points(user_id):
                bot.send_message(user_id, "Поздравляю! Это был последний вопрос на уровне!")
                bot.send_photo(message.chat.id, get_cat())
                get_questions_handler(message)
                bot.send_message(user_id, "Собери слово или словосочетание из ключиков, собранных на уровне! И напиши их в ответ!")
                bot.register_next_step_handler(message, check_key)
            else:
                bot.send_message(user_id, "Выбери следуюший вопрос по команде /questions")
        pass
    else:
        bot.send_message(user_id, "Ответ неверный, попробуй еще раз или выбери другой вопрос", reply_markup=gen_answer_markup(id))

@bot.message_handler(commands=['key'])
def handler_check_key(message):
    bot.send_message(message.chat.id, "Напиши собранное слово:")
    bot.register_next_step_handler(message, check_key)

def check_key(message):
    user_id = message.chat.id
    key = manager.get_level_key(user_id)
    if message.text.upper() == key:
        bot.send_message(user_id, "Ура! У тебя получилось! ")
        manager.add_bonus(user_id)
        last_level = manager.update_level(user_id)
        if last_level:
            bot.send_message(user_id, "Поздравляю! Ты прошел игру, теперь ты модешь отслеживать рейтинг по команде /rating")
        else:
            bot.send_message(user_id, "Идем дальше! Новый уровень тебе открылся!")
    else:
        bot.send_message(user_id, "Это неверный ответ :( Попробуй еще раз по команде /key")
        

@bot.message_handler(commands=['start'])
def start(message):
    bot.send_message(message.chat.id, """Добро пожаловать! Чтобы зарегистрировать команду, введите /register
Для просмотра рейтинга введи: /rating
Для просмотра вопросов: /questions""")


@bot.message_handler(commands=['register'])
def register(message):
    markup = ReplyKeyboardMarkup(row_width=2, one_time_keyboard=True)
    markup.add(KeyboardButton('Да'), KeyboardButton("Нет"))
    bot.send_message(message.chat.id, "У вас уже есть команда?", reply_markup=markup)
    bot.register_next_step_handler(message,register_step1)


def register_step1(message):
    teams = manager.get_teams_name()
    if message.text == "Да" and teams:
        bot.send_message(message.chat.id, 'Выберите название команды: ', reply_markup=gen_teams_markup(teams))
    else: 
        bot.send_message(message.chat.id, "Введите название команды: ")
        bot.register_next_step_handler(message,register_step3)


def register_step3(message):
    team_id = manager.insert_team(message.text)
    manager.insert_user(message.chat.id, team_id)
    register_step4(message)


def register_step4(message):
    bot.send_message(message.chat.id, "Вы зарегистрированы. Начнем квиз!")
    bot.send_message(message.chat.id, """Прежде чем начать, небольшая инструкция:

Сегодня тебе предстоит потренироваться свои знания в 16 вопросах""")
    bot.send_message(message.chat.id, "Удачи! У тебя все получится🔥")
    bot.send_message(message.chat.id, "Введи команду /questions и посмотри доступные вопросы!")


@bot.message_handler(commands=['rating'])
def get_rating(message):
    rating = manager.get_rating() 
    bot.send_message(message.chat.id, 'Рейтинг команд (место, название команды, очки):', reply_markup=gen_rating_markup(rating))


@bot.message_handler(commands=['questions'])
def get_questions_handler(message):
    chat_id = message.chat.id
    bot.send_message(chat_id, 'Список вопросов', reply_markup=gen_questions_markup(manager.get_answers(chat_id))) 


if __name__ == '__main__':
    manager = DB_Manager(DATABASE)
    bot.polling()
    
