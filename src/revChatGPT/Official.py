"""
A simple wrapper for the official ChatGPT API
"""
from os import environ

import openai
import pymysql
import pandas as pd

class Chatbot:
    """
    Official ChatGPT API
    """

    def __init__(self, api_key: str) -> None:
        """
        Initialize Chatbot with API key (from https://platform.openai.com/account/api-keys)
        """
        openai.api_key = api_key or environ.get("OPENAI_API_KEY")
        self.prompt = Prompt()

    def ask(self, request: str) -> dict:
        """
        Send a request to ChatGPT and return the response
        """
        prompt = self.prompt.construct_prompt(request)
        completion = openai.Completion.create(
            engine="text-chat-davinci-002-20230126",
            prompt=prompt,
            temperature=0.5,
            max_tokens=1024,
            stop=["\n\n\n"],
        )
        if completion.get("choices") is None:
            raise Exception("ChatGPT API returned no choices")
        if len(completion["choices"]) == 0:
            raise Exception("ChatGPT API returned no choices")
        if completion["choices"][0].get("text") is None:
            raise Exception("ChatGPT API returned no text")
        completion["choices"][0]["text"] = completion["choices"][0]["text"].replace(
            "<|im_end|>",
            "",
        )
        # Add to chat history
        self.prompt.add_to_chat_history(
            "User: "
            + request
            + "\n\n\n"
            + "ChatGPT: "
            + completion["choices"][0]["text"]
            + "\n\n\n",
        )
        return completion


class Prompt:
    """
    Prompt class with methods to construct prompt
    """

    def __init__(self) -> None:
        """
        Initialize prompt with base prompt
        """
        self.base_prompt = (
            environ.get("CUSTOM_BASE_PROMPT")
            or "You are ChatGPT, a large language model trained by OpenAI. You answer as concisely as possible for each response (e.g. Don't be verbose).\n"
        )
        # Track chat history
        self.chat_history: list = []

    def add_to_chat_history(self, chat: str) -> None:
        """
        Add chat to chat history for next prompt
        """
        self.chat_history.append(chat)

    def history(self) -> str:
        """
        Return chat history
        """
        return "\n\n\n\n".join(self.chat_history)

    def construct_prompt(self, request: str) -> str:
        """
        Construct prompt based on chat history and request
        """
        prompt = self.base_prompt + self.history() + "User: " + request + "\nChatGPT:"
        # Check if prompt over 4000*4 characters
        if len(prompt) > 4000 * 4:
            # Remove oldest chat
            self.chat_history.pop(0)
            # Construct prompt again
            prompt = self.construct_prompt(request)
        return prompt

class MysqlHandler:
    """
    Prompt class with methods to deal with Mysql Server
    """
    def __init__(self) -> None:
        """
	Initialize with mysql server config
        """
        self.conn = pymysql.connect(host='10.104.56.19',
                     user='root',
		     port=8306,	
                     database='olap')

    def convert(self, completion):
        """
        convert chatGPT response to sql
        """
        oritext = completion["choices"][0]["text"]
        oritext = oritext.replace("```", "")
        oritext = oritext.replace("\n"," ")
        return oritext

    def sql(self, query):
        cursor = self.conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        return result

    def query(self, sql):
        df = pd.read_sql(sql, self.conn)
        print(df.head())
        #print(df.describe())
        #df.plot()

"""
chatGPT demo response
{
  "choices": [
    {
      "finish_details": {
        "stop": "<|endoftext|>",
        "type": "stop"
      },
      "index": 0,
      "logprobs": null,
      "text": "\n```\nSELECT SUM(impressions) as total_impressions, SUM(cost) as total_cost\nFROM account_data\nWHERE date BETWEEN DATE_SUB(NOW(), INTERVAL 3 MONTH) AND NOW();\n```"
    }
  ],
  "created": 1675237497,
  "id": "cmpl-6f25Zxp0oCRbGwDX2G7yMtz7s9yQ9",
  "model": "text-chat-davinci-002-20230126",
  "object": "text_completion",
  "usage": {
    "completion_tokens": 59,
    "prompt_tokens": 97,
    "total_tokens": 156
  }
}
"""
api_token = '<TOKEN>'
chatgpt = Chatbot(api_token)
mysqlhandler = MysqlHandler()

test_sql1 = """
SELECT userinfo.name AS Account_Name, \n
       planinfo.name AS Plan_Name,\n
       olapPlanStats.date AS Date,\n 
       SUM(olapPlanStats.pageviews) AS Pageviews, \n
       SUM(olapPlanStats.pclicks) AS PClicks, \n
       SUM(olapPlanStats.pay) AS Pay\n
FROM olapPlanStats\n
JOIN planinfo ON olapPlanStats.planid = planinfo.planid AND olapPlanStats.userid = planinfo.userid\n
JOIN userinfo ON olapPlanStats.userid = userinfo.userid\n
WHERE olapPlanStats.date BETWEEN NOW() - INTERVAL 7 DAY AND NOW()\n
  AND olapPlanStats.type = 1\n
  AND userinfo.industry = '零售'\n
GROUP BY userinfo.name, olapPlanStats.date, planinfo.name\n
"""

model_text = """
我有三个数据表，它们的schema分别用XML来定义如下：
<table name="olapPlanStats" database="OLAP" source='olap'>
          <column name="userid" comment="账户id" type="uint" key="true"/>
          <column name="date" comment="日期" type="date"  key="true"/>
          <column name="planid" comment="计划id" type="ulong"  key="true"/>
          <column name="type" comment="流量类型" type="int"  key="true"/>
          <column name="pageviews" comment="展现" type="long"/>
          <column name="pclicks" comment="点击" type="long"/>
          <column name="pay" comment="消费" type="long"/>
  </table>     
这是表，其中流量类型列，1代表凤巢，2代表信息流

<table name="userinfo" database="FC_Feed" source='mysqlf1'>
        <column name="userid" comment="账户id" type="uint" key="true"/>
        <column name="name" comment="账户名称" type="string"/>
        <column name="industry" comment="行业" type="string"/>
    </table>   
这是账户表，可以根据账户id查询账户名称和行业，其中行业列，有3个不同的取值，分别是零售，医疗，房地产。

<table name="planinfo" database="FC_Feed" source='mysqlf1'>
        <column name="userid" comment="账户id" type="uint" key="true"/>
        <column name="planid" comment="计划id" type="ulong"/>
        <column name="name" comment="计划名称" type="string"/>
</table>
这是计划表，可以根据计划id查询计划名称
"""
#answer = mysqlhandler.sql(test_sql1)
#print(answer)
print("===========输入数据集给ChatGPT，进行训练=======")
answer = chatgpt.ask(model_text)
print(answer)
print("============模型训练完毕，可以开始对话==========")
ask_text1 = "请写一个满足Mysql语法的SQL，查询所有零售行业的账户，最近7天在凤巢流量上的所有展现，点击和消费，数据需要分日，分计划名称，分账户名称进行展示和聚合"
print(ask_text1)
answer = chatgpt.ask(ask_text1)
print(answer)
sql1 = mysqlhandler.convert(answer)
print("convert SQL:" + sql1)
answer = mysqlhandler.query(sql1)
while True:
    ask = input("请输入：")
    ans = chatgpt.ask(ask)
    sql = mysqlhandler.convert(ans)
    print("生成的查询SQL：" + sql)
    mysqlhandler.query(sql)
