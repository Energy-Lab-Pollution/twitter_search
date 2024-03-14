

def helper_split_setence(users):
    """
    This helper function takes a dictionary representing a park and extracts
    and splits sentences from values of "name", "history", "description"

    Parameters:
        park:  a dictionary representing a park

    Returns:
        A list of uncleaned tokens extracted from specified keys.
    """
    for user in users:
        temp_list = []
        for key, values in user.items():
            if key in ["name", "location", "description"]:
                try:
                    temp_list += values.strip().split()
                except Exception as e:
                    print(f"Error splitting sentence for {key}: {e}")
                    temp_list += []

        user['token'] = temp_list

    return users

def helper_clean_words(users):
        """
        This helper function takes a list of uncleaned tokens and cleans 
        them by converting to lowercase, removing punctuation, and 
        filtering out words in INDEX_IGNORE.

        Parameters:
            token_unclean:  a list of uncleaned tokens

        Returns:
            A list of cleaned tokens.
        """
        
        for dict in users:
            final_word = []
            for word in dict["token"]:
                try:
                    word = word.lower()
                    for char in word:
                        if char in PUNC:
                            word = word.replace(char,"")
                    if word not in INDEX_IGNORE:
                        final_word.append(word)
                except:
                    continue
            dict["token"]=  final_word
        return users


def tokenize(users):
    """
    This function takes a list of dictionary representing a park and returns a
    list of dictionaries that can be used to search for the park.

    The tokens should be a combination of the park's name, history, and
    description.

    All tokens should be normalized to lowercase, with punctuation removed as
    described in README.md.

    Tokens that match INDEX_IGNORE should be ignored.

    Parameters:
        * park:  a dictionary representing a park

    Returns:
        A list of tokens that can be used to search for the park.
    """
    #users_translated = translate_to_english(users)
    users_token_unclean = helper_split_setence(users)

    final_users = helper_clean_words(users_token_unclean)

    return final_users

def analyze_user_profile(name, description):

    nlp = spacy.load("en_core_web_sm")
    name_doc = nlp(name.lower())
    description_doc = nlp(description.lower())
    ORGANIZATION_KEYWORDS = ["official", "company", "organization", "corporation", 'collective', 'group', 'association', 'enterprise', 'foundation', 'institute', 'team', 'society', 'union', 'network', 'coalition', 'syndicate', 'consortium', 'firm', 'club', 'guild', 'committee', 'bureau', 'agency', 'cooperative', 'office', 'sector', 'service', 'branch', 'division', 'subsidiary', 'affiliate', 'wholesaler', 'retailer', 'supplier', 'manufacturer', 'distributor', 'seller', 'vendor', 'merchant', 'store', 'boutique', 'shop', 'marketplace', 'mart', 'emporium', 'shoppe', 'nonprofit', 'charity', 'NGO', 'non-governmental organization', 'advocacy', 'humanitarian', 'volunteer', 'philanthropy', 'community', 'society', 'coalition', 'alliance', 'initiative', 'campaign', 'movement', 'project', 'network', 'consortium', 'union', 'association', 'cooperative', 'collective', 'group', 'committee', 'team', 'council', 'partnership', 'collaborative', 'forum', 'guild', 'federation', 'civic', 'public service', 'social impact', 'sustainable', 'environmental', 'social responsibility', 'community service', 'global', 'international', 'worldwide', 'multinational', 'transnational', 'globalized', 'government', 'govt', 'gov', 'public sector', 'office of']

    for keyword in ORGANIZATION_KEYWORDS:
        if keyword in name_doc.text or keyword in description_doc.text:
            return "Organization"

    return "Individual"

def filter_df_by_keywords(df, keywords):
    filtered_df = [row.list_object for row in df.itertuples() if
                   any(keyword in row.list_object.name.lower() \
                       for keyword in keywords)]
    return filtered_df
