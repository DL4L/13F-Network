
# coding: utf-8


from py2neo import Graph, Node, Relationship, GraphService
import os
from tqdm import tqdm
import re

NEO4J_USER = 'neo4j'
NEO4J_PASSWORD = '123456'
NEO4J_HOST = 'localhost'


class NeoGraph:
    def __init__(self, gcp=None):
        if gcp:
            #self.g = Graph('bolt://neo4j:123456@35.230.134.163:7687')
            self.g = Graph(host=gcp['NEO4J_HOST'], port=gcp['NEO4J_PORT'], user=gcp['NEO4J_USER'],
                           password=gcp['NEO4J_PASSWORD'])
        else:
            self.g = Graph(host=NEO4J_HOST, user=NEO4J_USER,
                           password=NEO4J_PASSWORD)

    def truncate(self):
        """Remove all nodes in the graph"""
        print("----- Truncating graph -----")
        tx = self.g.begin()
        result = tx.run('MATCH (n) DETACH DELETE n')
        tx.commit()
        return result

    def add_companies(self, df):
        print("----- Starting Add companies process -----")
        tx = self.g.begin()
        for _, x in tqdm(df.iterrows(), total=len(df)):
            if x['ticker'] != "NA":
                n = Node("Ticker", name=x['ticker'], company=x['name'],
                         sector=x['sector'], variation_coefficient=x['var_coef'])
            tx.create(n)
        tx.commit()
        self.g.run("CREATE INDEX ON :Ticker(name)")
        print("----- Add companies process complete -----")

    def create_links(self, df):
        print("----- Starting relationship creation process -----")
        for _, x in tqdm(df.iterrows(), total=df.shape[0]):
            cypher = f"MATCH (s1:Ticker {{name:\'{x['ticker1']}\'}}),(s2:Ticker {{name:\'{x['ticker2']}\'}}) CREATE (s1)-[:CORR {{corr : {x['cor']}, id : '{x['id']}'}}]->(s2)"
            self.g.run(cypher)
        print("-----Relationship creation process complete -----")

    def add_tickers(self, df):
        print("----- Starting Add companies process -----")
        tx = self.g.begin()
        for _, x in tqdm(df.iterrows(), total=len(df)):

            n = Node("Ticker", ticker=x['ticker'], company=x['name'],
                     sector=x['sector'], )
            tx.create(n)
        tx.commit()
        self.g.run("CREATE INDEX ON :Ticker(ticker)")
        print("----- Add companies process complete -----")

    def add_funds(self, df):
        print("----- Starting Add Funds process -----")
        tx = self.g.begin()
        for _, x in tqdm(df.iterrows(), total=len(df)):

            n = Node("Fund", name=x['name'])
            tx.create(n)
        tx.commit()
        print("----- Add Funds process complete -----")

    def link_funds_to_tickers(self, funds_tickers):
        print("----- Starting relationship creation process -----")
        for fund in tqdm(funds_tickers, total=len(funds_tickers)):
            print(fund)
            for x in funds_tickers[fund]:
                if x['TICKER'] and x['VALUE'] > 0:

                    if re.sub('[^a-zA-Z]+', '', x['PUT/CALL']):
                        pc = re.sub('[^a-zA-Z]+', '',
                                    x['PUT/CALL']).upper()
                        cypher = f"MATCH (f1:Fund {{name:\'{fund}\'}}),(t1:Ticker {{ticker:\'{x['TICKER']}\'}}) CREATE (f1)-[:INVESTMENT {{valuex$1000 : {x['VALUE']}, shares : {x['SHARES']}, put_call : '{pc}'}}]->(t1)"
                    else:
                        cypher = f"MATCH (f1:Fund {{name:\'{fund}\'}}),(t1:Ticker {{ticker:\'{x['TICKER']}\'}}) CREATE (f1)-[:INVESTMENT {{valuex$1000 : {x['VALUE']}, shares : {x['SHARES']}}}]->(t1)"
                    try:
                        self.g.run(cypher)
                    except:
                        print("Failed ", fund, x)
        print("-----Relationship creation process complete -----")
