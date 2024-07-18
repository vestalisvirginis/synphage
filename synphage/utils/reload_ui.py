import requests

def reload_definitions():
    url = "http://localhost:3000/graphql"
    query = """
    mutation {
        reloadWorkspace {
            __typename
        }
    }
    """
    response = requests.post(url, json={'query': query})
    if response.status_code == 200:
        print("Definitions reloaded successfully")
    else:
        print("Failed to reload definitions")

