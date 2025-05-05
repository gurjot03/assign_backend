import json
from shl1 import AssessmentSearchSystem
import time
from dotenv import load_dotenv
import os
load_dotenv()

mongodb_uri = 'mongodb+srv://ripsjaw:ripsjaw123@cluster0.1opiorp.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0'
# mongodb_uri = os.getenv('MONGODB_URI')

def calculate_metrics(actual, predicted, k):
    if not actual:
        return 0.0, 0.0
    
    predicted_at_k = predicted[:k]
    
    relevant_retrieved = len(set(predicted_at_k) & set(actual))
    recall = relevant_retrieved / len(actual)
    
    precision = relevant_retrieved / k if k > 0 else 0
    
    return recall, precision

def calculate_ap(actual, predicted, k):
    if not actual:
        return 0.0
    
    relevant_count = 0
    running_sum = 0.0
    
    for i, item in enumerate(predicted[:k], 1):
        if item in actual:
            relevant_count += 1
            precision_at_i = relevant_count / i
            running_sum += precision_at_i
    
    return running_sum / len(actual) if actual else 0.0

def evaluate_search_system(test_queries, k_values=[5, 10]):
    results = {}
    
    for k in k_values:
        total_recall = 0
        total_map = 0
        query_count = len(test_queries["queries"])
        
        print(f"\nEvaluating for k={k}:")
        print("-" * 50)
        
        for i, query_data in enumerate(test_queries["queries"], 1):
            try:
                query = query_data["query"]
                actual_assessments = query_data["assessments"]
        
                search_system = AssessmentSearchSystem(mongodb_uri)
                search_results = search_system.search(query, limit=k)
                predicted_assessments = [result["name"] for result in search_results]
                
                recall, _ = calculate_metrics(actual_assessments, predicted_assessments, k)
                ap = calculate_ap(actual_assessments, predicted_assessments, k)
                
                total_recall += recall
                total_map += ap
                
                print(f"\nQuery {i}:")
                print(f"Query: {query[:100]}...")
                print(f"Recall@{k}: {recall:.3f}")
                print(f"AP@{k}: {ap:.3f}")
                print(f"Expected: {actual_assessments}")
                print(f"Predicted: {predicted_assessments}")
                
                # Sleep to avoid rate limiting
                time.sleep(20)
                
            except Exception as e:
                print(f"\nError processing query {i}: {str(e)}")
                continue
        
        mean_recall = total_recall / query_count
        mean_ap = total_map / query_count
        
        results[k] = {
            "mean_recall": mean_recall,
            "map": mean_ap
        }
        
        print(f"\nOverall Results for k={k}:")
        print(f"Mean Recall@{k}: {mean_recall:.3f}")
        print(f"MAP@{k}: {mean_ap:.3f}")
    
    return results

def main():
    try:
        with open("shl_test.json", "r") as f:
            test_queries = json.load(f)
        
        results = evaluate_search_system(test_queries)
        
        with open("evaluation_results.json", "w") as f:
            json.dump(results, f, indent=2)
            
    except Exception as e:
        print(f"Error during evaluation: {str(e)}")

if __name__ == "__main__":
    main()
