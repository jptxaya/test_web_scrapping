import requests as rq
from bs4 import BeautifulSoup
import datetime as dt
import re
from pymongo import MongoClient
import credentials_db as cred
import logging
from airflow.exceptions import AirflowFailException




def get_new_records(year,month,recipe_links):
    response = rq.get("https://www.directoalpaladar.com/archivos/"+ str(year)+"/"+str(month))
    soup = BeautifulSoup(response.content, "html.parser")
    for ref in [a.get('href') for a in soup.find_all('a') if a.get('href')]:
        if  ref.startswith("/recetas-") or ref.startswith("/postres") or ref.startswith("/recetario"):
            recipe_links.add(ref)
    return recipe_links


def get_elaboration_times(content,html_class):
    time = None
    time_html = content.find_all("li",class_= html_class)
    if len(time_html):
        auxTime = time_html.pop().find("span",class_="asset-recipe-time-value")
        time = auxTime.text.strip()
    return time

def get_ingredients_preparation(content):
    ingredients = []
    preparation = None
    ingredient_html = content.find_all("li",class_="asset-recipe-list-item m-is-ingr")
    if len(ingredient_html) == 0:
         article_sections = content.find_all("p")
         article_sections = [art.text for art in article_sections]
         ingredient_index = None
         prep_index = None
         end_preparation = len(article_sections) - 1 
         for i in range(len(article_sections)):
            if ((article_sections[i].startswith("Los ingredientes")) or (article_sections[i].startswith("Ingredient"))):
                 ingredient_index = i
            elif (article_sections[i].startswith("La preparación")):
                 prep_index = i
            elif (article_sections[i].startswith("En Directo al Paladar")) or ( article_sections[i].startswith("Los mejores comentarios")):
                min_prep = lambda x,y: x if x < y else y
                end_preparation = min_prep(i,end_preparation)
         if ingredient_index:
            ingredients = [ing.replace("Los ingredientes\n","").replace("Ingredientes:\n","").title().strip() for ing in re.split(",(?![^(]*\))",article_sections[ingredient_index])]
         if prep_index:
            preparation = "\n".join(article_sections[prep_index:end_preparation])
            preparation = preparation.replace("La preparación\n","").strip()
    else:
        ingredients = []
        ingredient_html = content.find_all("li",class_="asset-recipe-list-item m-is-ingr")
        if len(ingredient_html):
            for ingr in ingredient_html:
                ingredient_name = ingr.find("span",class_="asset-recipe-ingr-name")
                ingredient_amount = ingr.find("span",class_="asset-recipe-ingr-amount")
                ingredients.append(" ".join([ingredient_amount.text.strip().title(),ingredient_name.text.strip().title()]))
        ingredient_html = content.find_all("li",class_="asset-recipe-list-item m-is-ingr m-no-amount")
        if len(ingredient_html):
            for ingr in ingredient_html:
                ingredient_name = ingr.find("span",class_="asset-recipe-ingr-name")
                ingredients.append(ingredient_name.text.strip().title())
        prep_html = content.find_all("div",class_="asset-recipe-steps")
        if len(prep_html):
            steps_html = prep_html.pop().find_all("p")
            steps = []
            for step in steps_html:
                steps.append(step.text.strip())
            preparation = " ".join(steps)
    return ingredients, preparation

def get_RecipesInformation(recipes_links):
    recipeList = []
    print(f"Total recipes to process:{len(recipes_links)}")
    for recipe in recipes_links:
        try:
            recipeDict = dict()
            logging.info(f"RECIPE:{recipe}")
            response = rq.get("https://www.directoalpaladar.com"+recipe)
            soup = BeautifulSoup(response.content, "html.parser")
            article_date = None
            author_name = None
            difficulty = None
            totalTime = None
            prepTime = None
            cookTime = None
            persons = None
            ingredients = None
            preparation = None

            try:
                recipe_name = recipe[recipe.find("/",1)+1:].replace("-"," ").title()
            except:
                logging.error(f"Error getting Recipe Name {recipe}")
                recipe_name = None

            try:
                dates_hmtl = soup.find_all("time",class_="article-date")
                if len(dates_hmtl):
                        article_date = dates_hmtl[0].text.strip()[:10]
                        article_date = dt.datetime(int(article_date[:4]),int(article_date[5:7]),int(article_date[8:]))
                else:
                    article_date = None
            except:
                logging.error(f"Error getting Article Date {recipe}")
                article_date = None
            
            try:
                category = re.findall("/[\-a-z]*/",recipe).pop()[1:-1].replace("-"," ").title()
            except:
                logging.error(f"Error getting Category {recipe}")
                category = None
            
            try:
                authorName_html = soup.find_all("p",class_="author-name").pop()
                if len(authorName_html):
                    author_name = authorName_html.text
            except:
                logging.error(f"Error getting Author Name in {recipe}")

            try:
                dificulty_html = soup.find_all("div",class_="asset-recipe-difficulty")
                if len(dificulty_html):
                    difficulty = dificulty_html.pop().text.replace("Dificultad: ","")
            except:
                logging.error(f"Error getting Difficulty {recipe}")
            try:
                totalTime = get_elaboration_times(soup,"asset-recipe-list-item m-is-totaltime")
                prepTime = get_elaboration_times(soup,"asset-recipe-list-item m-is-preptime")
                cookTime = get_elaboration_times(soup,"asset-recipe-list-item m-is-cooktime")
            except:
                logging.error(f"Error getting Times {recipe}")    

            try:
                persons_html = soup.find_all("div",class_="asset-recipe-yield")
                if len(persons_html):
                    persons = str.strip(persons_html.pop().text.replace("Para ","").replace("\n",""))
            except:
                logging.error(f"Error getting Persons {recipe}")
            try:
                ingredients, preparation = get_ingredients_preparation(soup)
            except:
                logging.error(f"Error getting Ingredients and preparation {recipe}")
        except:
            logging.error(f"ERROR:{recipe}")
        else:
            if (len(ingredients) > 0):
                recipeDict = {"recipeName": recipe_name,
                        "recipeURL": "https://www.directoalpaladar.com"+recipe,
                        "category":category,
                        "articleDate": article_date,
                        "authorName": author_name,
                        "difficulty": difficulty,
                        "totalTime": totalTime,
                        "prepTime": prepTime,
                        "cookTime": cookTime,
                        "persons": persons,
                        "ingredients": ingredients,
                        "elaborationText": preparation
                        }
                recipeList.append(recipeDict)
    return recipeList

def getLastUpdateDate():
    #Connection to MongoDB
    mongoClient = MongoClient(cred.uri_mongo)
    db_mongo = mongoClient["project_recipes"]
    recipe_collection = db_mongo["recipes"]

    creation_status = False #Variable to control the first time we create the DB
    result_updateDate = recipe_collection.find_one({"lastYearMonth":{"$exists": True}},{"_id":0})
    if result_updateDate:
        startYear = int(result_updateDate["lastYearMonth"][:4])
        startMonth = int(result_updateDate["lastYearMonth"][4:])
        logging.info(f"LastYearMonth:{result_updateDate["lastYearMonth"]}")
    else:
        startYear = 2005 # For Initial load only
        startMonth = 1
        creation_status = True
    mongoClient.close()
    return  {"startYear": startYear,
             "startMonth": startMonth,
             "creation_status": creation_status
            }


def getRecipeLinks(startYear, startMonth):
    #Load of all_recipeURLs from the webpage
    recipes_links = set()
    lastYearMonthProcessed = "000000"
    for year in range(startYear,dt.datetime.now().year + 1):
        lastYearMonthProcessed = str(year) + lastYearMonthProcessed[4:6]
        for month in range(startMonth,13):
            lastYearMonthProcessed = lastYearMonthProcessed[:4] + (str(month) if month > 9 else "0"+str(month))
            recipes_links = get_new_records(year,month,recipes_links)
            if len(recipes_links) > 100:
                break
        if len(recipes_links) > 100:
            break
        if lastYearMonthProcessed[4:6] == "12":
            startMonth = 1
    if len(recipes_links) == 0:
        raise AirflowFailException(f"Error getting recipe links from {startYear}-{startMonth}")
    return recipes_links,lastYearMonthProcessed


def getRecipesAndStorage(**kwargs):
    xcom_value = kwargs["ti"].xcom_pull(task_ids="getLastUpdateDate")
    recipeLinks,lastYearMonthProcessed = getRecipeLinks(xcom_value["startYear"],xcom_value["startMonth"])
    recipeList = get_RecipesInformation(recipeLinks)
    #Connection to MongoDB
    mongoClient = MongoClient(cred.uri_mongo)
    db_mongo = mongoClient["project_recipes"]
    recipe_collection = db_mongo["recipes"]
    if xcom_value["creation_status"]:
         #Initial load
        result_update = recipe_collection.insert_one({"lastYearMonth":lastYearMonthProcessed})
        result_update = recipe_collection.insert_many(recipeList)
        print(f"Total recipes inserted:{len(recipeList)}")
    else:
        #Updating the document that contain the registry of the lastUpdate
        result_update = recipe_collection.update_one({"lastYearMonth":{"$exists": True}},{"$set":{"lastYearMonth":lastYearMonthProcessed}})
        count = 0
        for rl in recipeList:
            result_recipe = recipe_collection.find_one({"recipeName":rl["recipeName"]})
            if not result_recipe:
                count += 1
                inserted_recipe = recipe_collection.insert_one(rl)
        print(f"Total recipes inserted:{count}")
    mongoClient.close()
    print("Process finished")






