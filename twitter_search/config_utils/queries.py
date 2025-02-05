"""
Dictionary with the current queries for searching users
"""
# QUERY DICTIONARY
QUERIES = {
    "media": """location (media OR press OR coverage OR broadcasting
                OR (breaking news) OR journalism OR journalist
                OR local OR news OR patrika) -is:retweet""",
    "organizations": """((NGO location) OR (organization location)
                       OR (non-profit location) OR (location institution) OR
                       (non-governmental organization location) OR (nonprofit location))
                       -is:retweet""",
    "policymaker": """location ((member of parliament) OR governor OR minister
                    OR magistrate OR (district magistrate) OR IAS OR officer
                    OR cabinet OR mayor OR councillor OR (local government)
                    OR (city official) OR (MLA) OR (MP) OR government OR municipality)
                    -is:retweet""",
    "politicians": """location (politics OR politicians OR politician) -is:retweet""",
    "researcher": """location ((environmental research) OR (environmental researcher)
                    OR science OR academic OR research OR university OR professor OR
                    postdoc OR postdoctoral OR PhD)
                    -is:retweet""",
    "environment": """location ((air pollution) OR pollution OR (public health)
                OR (poor air) OR asthma OR polluted OR (pollution control board)
                OR smog OR (air quality)) -is:retweet""",
}


QUERIES_ES = {
    "media": """location (media OR prensa OR periodico OR transmision
                          OR periodismo OR periodista
                          OR local OR noticias) -is:retweet""",
    "organizations": """location ("NGO" OR "organizacion" OR "organizaciones" OR
                     "organizacion civil" OR "non-profit" OR "institucion" OR
                     "sin fines de lucro" OR "non-governmental organization" OR
                     "nonprofit") -is:retweet""",
    "policymaker": """location (gobernador OR ministro OR magistrado OR diputado
                    OR senador OR gabinete OR (gobierno local) OR gobierno OR
                    municipio OR delegacion OR ministerio OR legislador OR legisladores
                    OR legislacion OR legisladora)
                    -is:retweet""",
    "politicians": """location (politica OR politicos OR politico OR politicas)
                      -is:retweet""",
    "researcher": """location ((investigacion ambiental) OR (investigador ambiental)
                    OR (investigadora ambiental) OR ciencia OR academico
                    OR academica OR investigacion OR universidad OR profesor
                    OR postdoc OR postdoctoral OR PhD OR doctorado OR investigador
                    OR investigadora OR universitario OR universitaria) -is:retweet""",
    "environment": """location ((contaminacion del aire) OR contaminacion OR
                    (salud publica) OR (poor air) OR asma OR contaminado OR contaminada
                    OR smog OR (calidad del aire) OR (medio ambiente)) -is:retweet""",
}


QUERIES_FR = {
    "media": """location (média OR presse OR couverture OR diffusion
                OR (flash info) OR journalisme OR journaliste
                OR local OR actualités) -is:retweet""",
    "organizations": """((ONG location) OR (organisation location)
                       OR (non lucratif location) OR (location institution) OR
                       (organisation non gouvernementale location) OR (non lucratif location))
                       -is:retweet""",
    "policymaker": """location ((membre du parlement) OR gouverneur OR ministre
                    OR magistrat OR (magistrat de district) OR IAS OR officier
                    OR cabinet OR maire OR conseiller municipal OR (gouvernement local)
                    OR (fonctionnaire municipal) OR (MLA) OR (député) OR gouvernement OR municipalité)
                    -is:retweet""",
    "politicians": """location (politique OR politiciens OR politicien) -is:retweet""",
    "researcher": """location ((recherche environnementale) OR (chercheur environnemental)
                    OR science OR universitaire OR recherche OR université OR professeur OR
                    postdoc OR postdoctoral OR doctorat)
                    -is:retweet""",
    "environment": """location ((pollution de l'air) OR pollution OR (santé publique)
                OR (air de mauvaise qualité) OR asthme OR pollué OR (commission de contrôle de la pollution)
                OR smog OR (qualité de l'air)) -is:retweet""",
}



QUERIES_DICT = {"en": QUERIES, "es": QUERIES_ES, "fr": QUERIES_FR}
