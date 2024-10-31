import pandas as pd
import time
from Bio import Entrez
import os
import json
import re

# email address for NCBI
Entrez.email = "jgustavomartins@gmail.com"

# extract and format the publication date from the PubDate field
def parse_date_from_pubdate(pubdate):
    """
    Extracts the publication date from the PubDate field.
    
    The function does not use `pubdate.get('Year', '01')` for the `year` variable
    because it wants to ensure that a valid year is extracted from the `pubdate`
    dictionary. If the 'Year' key is not present, the function returns "No date found"
    instead of using a default value. This approach is more robust than relying on
    a default value, as it can properly handle cases where the 'Year' key is missing
    from the `pubdate` dictionary.
    """
    if 'Year' in pubdate:
        year = pubdate['Year']
        month = pubdate.get('Month', '01') # default to January
        day = pubdate.get('Day', '01') # default to 1
        return f'{year}-{month}-{day}'
    else:
        return "No date found"

# fetch pubmed data using an example query
ex_query = '(("critical illness"[MeSH Terms] OR ("critical"[All Fields] AND "illness"[All Fields]) OR "critical illness"[All Fields] OR ("critically"[All Fields] AND "ill"[All Fields]) OR "critically ill"[All Fields]) AND ("burns"[MeSH Terms] OR "burns"[All Fields] OR "burn"[All Fields]) AND ("patient s"[All Fields] OR "patients"[MeSH Terms] OR "patients"[All Fields] OR "patient"[All Fields] OR "patients s"[All Fields])) AND ((y_1[Filter]) AND (review[Filter]))'

def fetch_pubmed_data(query, max_results=50):
    Entrez.email = "your.email@example.com"  # Always set your email for Entrez
    handle = Entrez.esearch(db="pubmed", term=query, retmax=max_results)
    record = Entrez.read(handle)
    id_list = record["IdList"]
    return id_list

# fetch article details from pubmed ids
def fetch_article_details(pubmed_ids):
    handle = Entrez.efetch(db="pubmed", id=pubmed_ids, retmode="xml")
    records = Entrez.read(handle)
    return records

# create a pandas dataframe from the pubmed article details
def create_publication_dataframe(records):
    df = pd.DataFrame()
    for record in records['PubmedArticle']:
        # Print the record in a formatted JSON style
        print(json.dumps(record, indent=4, default=str))  # default=str handles types JSON can't serialize like datetime
        pmid = record['MedlineCitation']['PMID']
        title = record['MedlineCitation']['Article']['ArticleTitle']
        abstract = ' '.join(record['MedlineCitation']['Article']['Abstract']['AbstractText']) if 'Abstract' in record['MedlineCitation']['Article'] and 'AbstractText' in record['MedlineCitation']['Article']['Abstract'] else ''
        authors = ', '.join(author.get('LastName', '') + ' ' + author.get('ForeName', '') for author in record['MedlineCitation']['Article']['AuthorList'])
        
        affiliations = []
        for author in record['MedlineCitation']['Article']['AuthorList']:
            if 'AffiliationInfo' in author and author['AffiliationInfo']:
                affiliations.append(author['AffiliationInfo'][0]['Affiliation'])
        affiliations = '; '.join(set(affiliations))

        journal = record['MedlineCitation']['Article']['Journal']['Title']
        keywords = ', '.join(keyword['DescriptorName'] for keyword in record['MedlineCitation']['MeshHeadingList']) if 'MeshHeadingList' in record['MedlineCitation'] else ''
        url = f"https://www.ncbi.nlm.nih.gov/pubmed/{pmid}"

        new_row = pd.DataFrame({
            'PMID': [pmid],
            'Title': [title],
            'Abstract': [abstract],
            'Authors': [authors],
            'Journal': [journal],
            'Keywords': [keywords],
            'URL': [url],
            'Affiliations': [affiliations]
        })

        df = pd.concat([df, new_row], ignore_index=True)
    
    # Create the 'outputs' folder if it doesn't exist
    os.makedirs('outputs', exist_ok=True)
    
    # Save the DataFrame to a CSV file
    filename = time.strftime('%Y%m%d%H%M') + '.csv'
    df.to_csv(os.path.join('outputs', filename), index=False)
    
    return df

def create_publication_markdown(publication_df):
    """
    Creates a Markdown file for each publication in the input DataFrame.
    
    The Markdown file is named after the article title, with special characters removed.
    The file content includes the publication details formatted in Markdown, with headings
    and subheadings as needed. Author names are formatted within double square brackets,
    without any special characters.
    
    If a file with the same name already exists in the 'outputs' folder, it will be overwritten.
    """
    print("Starting create publication markdown function")
    for _, row in publication_df.iterrows():
        title = row['Title']
        # Remove special characters from the title to make it compatible with the file name
        filename = re.sub(r'[^a-zA-Z0-9\s]', '', title) + '.md'
        filepath = os.path.join('outputs', filename)
        
        markdown_content = f"# {row['Title']}\n\n"
        markdown_content += f"**PMID:** {row['PMID']}\n\n"
        markdown_content += f"**Journal:** {row['Journal']}\n\n"
        markdown_content += f"**Authors:** {'**, **'.join(['[[' + re.sub(r'[^a-zA-Z0-9\s]', '', author.strip()) + ']]' for author in row['Authors'].split(', ')])}\n\n"
        markdown_content += f"**Abstract:**\n{row['Abstract']}\n\n"
        markdown_content += f"**Keywords:** {row['Keywords']}\n\n"
        markdown_content += f"**URL:** {row['URL']}\n\n"
        markdown_content += f"**Affiliations:** {row['Affiliations']}\n\n"

        # Write to file
        with open(filepath, "w") as f:
            f.write(markdown_content)
    print("Finished creating markdown files")

# Example usage
if __name__ == "__main__":
    ex_ids = fetch_pubmed_data(ex_query)
    ex_records = fetch_article_details(ex_ids)
    ex_df = create_publication_dataframe(ex_records)
    create_publication_markdown(ex_df)
