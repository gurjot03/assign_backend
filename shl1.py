import pandas as pd
import numpy as np
from pymongo import MongoClient
import os
from dotenv import load_dotenv
import google.generativeai as genai
from sentence_transformers import SentenceTransformer
import json

load_dotenv()

class AssessmentSearchSystem:
    def __init__(self, mongodb_uri, collection_name="tests"):
        self.client = MongoClient(mongodb_uri)
        self.db = self.client['assessment_search']
        self.collection = self.db[collection_name]
        
        api_key = os.getenv("GEMINI_API_KEY")
        genai.configure(api_key=api_key)
        self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
        self.gemini_model = genai.GenerativeModel('gemini-2.0-flash-001')
    
    def create_document_text(self, row):
        document_text = f"""
        Name: {row.get('Name', '')}
        Description: {row.get('Description', '')}
        Test Type: {row.get('Test Type', '')} 
        Job Levels: {row.get('Job Levels', '')}
        Languages: {row.get('Languages', '')}
        Assessment Length: {row.get('Assessment Length', '')}
        """
        return document_text.strip()
    
    def generate_embedding(self, text):
        try:
            embedding = self.embedding_model.encode(text)
            return embedding.tolist()
        except Exception as e:
            print(f"Error generating embedding: {e}")
            return None
    
    def refine_query(self, query):
        prompt = f"""
        the user query is {query}
        You are a search query optimizer for an testing solutions database. 
        The database contains records with these fields:
        - Name: Testing solution names
        - Description: Detailed description of the test
        - Test Type: Categories like 'Ability & Aptitude','Biodata & Situational Judgement','Competencies','Development & 360','Assessment Exercises','Knowledge & Skills','Personality & Behavior','Simulations'
        - Job Levels: Target job levels (Entry-level, Mid-Professional, Manager, etc.)
        - Languages: Available languages
        - Assessment Length: Duration of the test ,if nothing is mentioned dont use this field. I query like about an hour then assessment length should be <=60
        If query like 30-40 mins then assessment length should be 30-40.
        If query like can be completed in 40 mins then assessment length should be <=40
        
        Always include description of the assessment in the refined query.
        The spoken languages in the query should be kept in the Languages field.
        The test types if many should be separated by commas.
        Given the following user query, extract key search terms and criteria relevant to finding 
        matching assessments. Format your response as a clean, refined search query that highlights 
        the most important requirements.Do not output any explanation and additional text, only give the refined query.

        Example - 

        Name: .NET MVC (New)
        Description: knowledge of Model-View-Controller (MVC) architecture, validation and .NET framework.
        Test Type: Knowledge & Skills 
        Job Levels: Mid-Professional, Professional Individual Contributor,
        Languages: English (USA),
        Assessment Length: 17
        """
        
        response = self.gemini_model.generate_content(prompt)
        refined_query = response.text.strip()
        return refined_query
    
    def process_csv_and_create_embeddings(self, csv_file):
        df = pd.read_csv(csv_file)
        print(f"Processing {len(df)} records from {csv_file}")
        for index, row in df.iterrows():

            document_text = self.create_document_text(row)
            
            embedding = self.generate_embedding(document_text)
            
            if embedding:
                document = {
                    'name': row.get('Name', ''),
                    'url': row.get('URL', ''),
                    'remote_testing': row.get('Remote Testing', ''),
                    'adaptive': row.get('Adaptive/IRT', ''),
                    'test_type': row.get('Test Type', ''),
                    'description': row.get('Description', ''),
                    'job_levels': row.get('Job Levels', ''),
                    'languages': row.get('Languages', ''),
                    'assessment_length': row.get('Assessment Length', ''),
                    'text': document_text,
                    'embedding': embedding
                }
                
                self.collection.update_one(
                    {'name': document['name']},
                    {'$set': document},
                    upsert=True
                )
            
            if (index + 1) % 50 == 0:
                print(f"Processed {index + 1} records...")
        
        print(f"Completed processing {len(df)} records.")

    def search(self, query, limit):
        refined = self.refine_query(query)
        print(refined)
        query_embedding = self.generate_embedding(refined)

        if not query_embedding:
            return []

        import re
        length_pattern = r'Assessment Length:\s*(<=|>=|)(\d+)(?:-(\d+)|)'
        length_match = re.search(length_pattern, refined)

        pipeline = [
            {
                "$vectorSearch": {
                    "index": "vector_index",
                    "path": "embedding",
                    "queryVector": query_embedding,
                    "numCandidates": 600,
                    "limit": 100
                }
            }
        ]

        if length_match:
            operator = length_match.group(1)
            first_num = float(length_match.group(2))
            second_num = length_match.group(3)

            if operator == "<=":
                length_filter = {"$lte": first_num}
            elif operator == ">=":
                length_filter = {"$gte": first_num}
            elif second_num: 
                length_filter = {
                    "$gte": first_num,
                    "$lte": float(second_num)
                }
            else: 
                length_filter = {"$eq": first_num}

            pipeline.append({
                "$match": {
                    "assessment_length": length_filter
                }
            })

        pipeline.append({
            "$project": {
                "_id": 0,
                "name": 1,
                "url": 1,
                "remote_testing": 1,
                "adaptive": 1,
                "test_type": 1,
                "description": 1,
                "job_levels": 1,
                "languages": 1,
                "assessment_length": 1,
                "score": {"$meta": "vectorSearchScore"}
            }
        })

        pipeline.append({"$limit": limit})

        try:
            results = list(self.collection.aggregate(pipeline))
            return results
        except Exception as e:
            print(f"Error during search: {e}")
            return []

def main():
    mongodb_uri = os.getenv('MONGODB_URI')
    
    search_system = AssessmentSearchSystem(mongodb_uri)
    
    # Process CSV file
    # csv_file = "/Users/gurjotsingh/try/shl_product_catalog_enriched2.csv"
    # search_system.process_csv_and_create_embeddings(csv_file)
    
    query = 'Find me 1 hour long assesment for the below job at SHL Job Description Join a community that is shaping the future of work! SHL, People Science. People Answers. Are you a seasoned QA Engineer with a flair for innovation? Are you ready to shape the future of talent assessment and empower organizations to unlock their full potential? If so, we want you to be a part of the SHL Team! As a QA Engineer, you will be involved in creating and implementing software solutions that contribute to the development of our groundbreaking products. An excellent benefit package is offered in a culture where career development, with ongoing manager guidance, collaboration, flexibility, diversity, and inclusivity are all intrinsic to our culture. There is a huge investment in SHL currently so there’s no better time to become a part of something transformational. What You Will Be Doing Getting involved in engineering quality assurance and providing inputs when required. Create and develop test plans for various forms of testing. Conducts and/or participates in formal and informal test case reviews. Develop and initiate functional tests and regression tests. Rolling out improvements for testing and quality processes. Essential What we are looking for from you: Development experience – Java or JavaScript, CSS, HTML (Automation) Selenium WebDriver and page object design pattern (Automation) SQL server knowledge Test case management experience. Manual Testing Desirable Knowledge the basic concepts of testing Strong solution-finding experience Strong verbal and written communicator. Get In Touch Find out how this one-off opportunity can help you achieve your career goals by making an application to our knowledgeable and friendly Talent Acquisition team. Choose a new path with SHL. #CareersAtSHL #SHLHiringTalent #TechnologyJobs #QualityAssuranceJobs #CareerOpportunities #JobOpportunities About Us We unlock the possibilities of businesses through the power of people, science and technology. We started this industry of people insight more than 40 years ago and continue to lead the market with powerhouse product launches, ground-breaking science and business transformation. When you inspire and transform people’s lives, you will experience the greatest business outcomes possible. SHL’s products insights, experiences, and services can help achieve growth at scale. What SHL Can Offer You Diversity, equity, inclusion and accessibility are key threads in the fabric of SHL’s business and culture (find out more about DEI and accessibility at SHL ) Employee benefits package that takes care of you and your family. Support, coaching, and on-the-job development to achieve career success A fun and flexible workplace where you’ll be inspired to do your best work (find out more LifeAtSHL ) The ability to transform workplaces around the world for others. SHL is an equal opportunity employer. We support and encourage applications from a diverse range of candidates. We can, and do make adjustments to make sure our recruitment process is as inclusive as possible. SHL is an equal opportunity employer.'
    results = search_system.search(query,10)
    
    print("\nSearch Results:")
    for i, result in enumerate(results):
        print(f"\n{i+1}. {result['name']} (Score: {result.get('score', 'N/A')})")
        print(f"   Type: {result['test_type']}")
        print(f"   Length: {result['assessment_length']}")
        print(f"   URL: {result['url']}")

if __name__ == "__main__":
    main()
