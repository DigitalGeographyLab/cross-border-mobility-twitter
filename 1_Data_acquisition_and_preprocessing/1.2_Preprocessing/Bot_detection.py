"""
A script for detecting bots from Twitter data and exluding them.

@author: smassine
"""

# Use Botometer for bot detection
# Read more: https://botometer.iuni.iu.edu/#!/
import botometer

# Define keys for authentification
mashape_key = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
twitter_app_auth = {
    'consumer_key': 'XXXXXXXXXXXXXXXXXXX',
    'consumer_secret': 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX',
    'access_token': 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX',
    'access_token_secret': 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX',
  }

# Configure Botometer for searches
bom = botometer.Botometer(wait_on_ratelimit=True, mashape_key=mashape_key, **twitter_app_auth)

# Read in the Twitter userid list, these will be run through the Botomere in order to find out how many bots there is in the data
userid_list = open(r"C:\LocalData\smassine\Gradu\Data\Twitter\FIN_EST\haettavat_useridt.txt", "r")
lines = userid_list.read().split('\n')

# Create a list for storing bot statistics
bot_list = []
x = 1

# Check a sequence of accounts
for userid, result in bom.check_accounts_in(lines):
        
    print("Processing... ", x, "/", len(lines))
    x = x + 1
    
    try:
        
        # Use 40 % probability as a therhold for a a bot
        if result['cap']['universal'] >= 0.4 or result['cap']['english'] >= 0.4:
            bot_element = {'userid': userid, 'cap_score': result['cap']}
            bot_list.append(bot_element)
            print("A possible bot detected! Userid: ", userid)
            print("Bot list length now: ", len(bot_list))
        
    except KeyError:
        print(result['error'])

# Save bot list to txt file
with open(r"C:\LocalData\smassine\Gradu\Data\Twitter\FIN_EST\bot_list.txt", "w") as output:
    output.write(str(bot_list))
    print("Successfully saved the bot list!")