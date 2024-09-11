from slack_bolt import App
from slack_bolt.adapter.aws_lambda import SlackRequestHandler

import re, time, os
from datetime import datetime
import mysql.connector

SLACK_APP_TOKEN=os.environ['SLACK_APP_TOKEN']
SLACK_BOT_TOKEN=os.environ['SLACK_BOT_TOKEN']
SLACK_SIGNING_SECRET=os.environ['SLACK_SIGNING_SECRET']
MYSQL_HOST=os.environ['MYSQL_HOST']
MYSQL_USER=os.environ['MYSQL_USER']
MYSQL_PASS=os.environ['MYSQL_PASS']
MYSQL_DB_NAME=os.environ['MYSQL_DB_NAME']

tag_regex = '<@[a-zA-Z0-9]{11}>'
date_regex = '[1]{0,1}[0-9]{1}/[123]{0,1}[0-9]{1}/24'

DEBUG = False
logger = None
if DEBUG:
	import logging
	logger = logging.getLogger()
	logger.setLevel(logging.DEBUG)


class DBConnector:
	# TODO: make table for each slack server
	#       table for all servers, their snipe channel, someone with admin
	def __init__(self):
		self.cnx = mysql.connector.connect(
			user=MYSQL_USER, 
			password=MYSQL_PASS, 
			host=MYSQL_HOST,
			database=MYSQL_DB_NAME
			)
		self.cursor = self.cnx.cursor()
		""" self.cursor.execute('CREATE TABLE if not exists `UserName` (      \
							`user_id` varchar(45) NOT NULL, \
							`name` varchar(45) NOT NULL     \
							) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;')
		self.cnx.commit() """
		self.cursor.execute('CREATE TABLE if not exists `UserUserSnipe` (   \
							`sniper_id` varchar(45) NOT NULL, \
							`victim_id` varchar(45) NOT NULL  \
							) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;')
		self.cnx.commit()
		self.cursor.close()
		self.cursor = self.cnx.cursor(prepared=True)
		print('db connected')

	def clear_snipes(self):
		self.cursor.execute('truncate UserUserSnipe;')
		self.cnx.commit()
	
	def insert_snipe(self, sniper_id, victim_id):
		stmt = 'insert into UserUserSnipe values (%s, %s);'
		self.cursor.execute(stmt, (sniper_id, victim_id,))
		self.cnx.commit()
		

	def get_leaderboard(self):
		stmt = 'select sniper_id, COUNT(*) \
				from UserUserSnipe group by sniper_id           \
				order by count(*) DESC limit 3;'
		self.cursor.execute(stmt)
		return self.cursor.fetchall()
	
	def get_user_stats(self, user_id):
		stmt = 'select COUNT(sniper_id) as \'sniper_count\' from UserUserSnipe where sniper_id=%s;'
		self.cursor.execute(stmt, (user_id,))
		sniper = self.cursor.fetchall()[0][0]

		stmt = 'select COUNT(victim_id) as \'sniped_count\' from UserUserSnipe where victim_id=%s;'
		self.cursor.execute(stmt, (user_id,))
		sniped = self.cursor.fetchall()[0][0]

		stmt = 'select victim_id from UserUserSnipe where sniper_id=%s \
				group by victim_id order by count(victim_id) desc limit 1; '
		self.cursor.execute(stmt, (user_id,))
		most_sniper = self.cursor.fetchall()
		if not most_sniper or len(most_sniper) < 1:
			most_sniper = None
		else:
			most_sniper = most_sniper[0][0]

		stmt = 'select sniper_id from UserUserSnipe where victim_id=%s \
				group by sniper_id order by count(sniper_id) desc limit 1; '
		self.cursor.execute(stmt, (user_id,))
		most_sniped = self.cursor.fetchall()
		if not most_sniped or len(most_sniped) < 1:
			most_sniped = None
		else:
			most_sniped = most_sniped[0][0]

		stmt = 'select rank() over (order by count(sniper_id) desc) as rank_no from UserUserSnipe where sniper_id=%s;'
		self.cursor.execute(stmt, (user_id,))
		rank = self.cursor.fetchall()[0][0]
		
		return (sniper, sniped, most_sniper, most_sniped, rank)
	
	# TODO: add support for transitive relations, split into two tables maybe?
	# TODO: integrate into leaderboard, sniped, etc.
	def link(self, user1_id, user2_id):
		stmt = 'insert into UserLink select (%s, %s)    \
				where not exists (                      \
					select * from UserLink              \
					where (user1_id=%s and user2_id=%s) \
					or (user1_id=%s and user2_id=%s)    \
				);'
		self.cursor.execute(stmt, (user1_id, user2_id, user1_id, user2_id, user2_id, user1_id,))
	
	# use to limit usage of API
	# currently no longer using
	def register_user(self, user_id, name):
		stmt = 'insert into UserName values (%s, %s);'
		self.cursor.execute(stmt, (user_id, name,))
	def get_user_from_id(self, user_id):
		stmt = 'select name from UserName where user_id=%s;'
		self.cursor.execute(stmt, (user_id, ))
		return self.cursor.fetchall()	
	
app = App(token=SLACK_BOT_TOKEN, 
		  signing_secret=SLACK_SIGNING_SECRET,
		  process_before_response=True)
print('app connected')
dbc = DBConnector()


def handler(event, context):
	if event.get('challenge'):
		challenge = event.get('challenge')
		return {
			"statusCode": 200,
			"headers": {"Content-Type": "application/json"},
			"body": {"challenge": challenge}
		}
	
	slack_handler = SlackRequestHandler(app=app)
	return slack_handler.handle(event, context)

@app.message(re.escape('+lb'))
def handle_leaderboard(say):
	print('+lb')
	lb = dbc.get_leaderboard()
	s=''
	for i in range(len(lb)):
		# TODO: use UserName to assign names to users, don't want to spam them with tags
		emojis = ['crown', 'two', 'three']
		s+=f':{emojis[i]}:: <@{lb[i][0]}>, {lb[i][1]} snipes\n'
	say(s)

@app.message(re.escape('+leaderboard'))
def handle_leaderboard_alt(say):
	handle_leaderboard(say)

@app.message(re.escape('+stats'))
def handle_stat_req(message, say):
	if DEBUG:
		logger.info('handling stats request')

	user = re.search(tag_regex, message['text'])
	if not user:
		say('need to provide me a user!')
		return
	
	user = user.group(0)
	stats = dbc.get_user_stats(user[2:-1])
	s=''
	s+=f'User <@{user[2:-1]}>:\n'
	s+=f'Ranking: {stats[4]}\n'
	s+=f'{stats[0]} Snipes\n'
	s+=f'{stats[1]} Times Sniped\n'
	s+=f'K/D: {round(max(stats[0], 1) / max(stats[1], 1), 2)}\n'
	s+=f'Most Sniped: <@{stats[2]}>\n' if stats[2] is not None else 'Never sniped anyone\n'
	s+=f'Most Sniped By: <@{stats[3]}>\n' if stats[3] is not None else 'Never been sniped'
	say(s)

@app.message(re.escape('+restart'))
def init_db(message, say):
	if DEBUG:
		logger.info('handling restart')

	if message['user'] != 'U06UVDGS4BY':
		say('insufficient permissions')

	try:
		init_ts = time.mktime(datetime.strptime(
					re.search(date_regex, message['text']).group(0), 
					'%m/%d/%y').timetuple())
	except:
		say('something is wrong with your date! Needs mm/dd/yy')
		return
	
	dbc.clear_snipes()
	history = app.client.conversations_history(channel=message['channel'])['messages']
	for item in history:
		if 'files' in item:
			tags = re.findall(tag_regex, item['text'])
			for tag in tags:
				if float(item['ts']) > init_ts:
					dbc.insert_snipe(item['user'], tag[2:-1])
			if tags is not None:
				try:
					app.client.reactions_add(
								channel=message['channel'], 
								name='sniped', 
								timestamp=item['ts']
							)
				except:
					pass
	if DEBUG:
		logger.info('updated from ' + datetime.fromtimestamp(float(init_ts)))

@app.message(tag_regex)
def handle_snipe(message):
	if DEBUG:
		logger.info('handling snipe')

	if message['user'] == 'U07JN5QMTRC':
		return
	
	# TODO: check if already posted/snipes in succession
	if 'files' in message:
		tags = re.findall(tag_regex, message['text'])
		for tag in tags:
			print(tag, message['event_ts'])
			dbc.insert_snipe(message['user'], tag[2:-1])
		res = app.client.reactions_add(
				channel=message['channel'], 
				name='sniped', 
				timestamp=message['event_ts']
			)
		print(res)

# needed to catch all
@app.event('message')
def handle_message():
	pass
	
	