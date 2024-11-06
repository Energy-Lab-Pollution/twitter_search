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
                    OR (city official) OR (MLA) OR (MP) OR governmment OR municipality)
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
    "organizations": """((NGO location) OR (organizacion location)
                       OR (non-profit location) OR (location institucion) OR
                       (non-governmental organization location) OR (nonprofit location))
                       -is:retweet""",
    "policymaker": """location ((member of parliament) OR gobernador OR ministro
                    OR magistrado OR diputado OR senador OR cabinete OR (gobierno local)
                    OR (MLA) OR (MP) OR gobierno OR municipio OR delegacion)
                    -is:retweet""",
    "politicians": """location (politica OR politicos OR politico OR politicas)
                      -is:retweet""",
    "researcher": """location ((investigacion ambiental) OR (investigador ambiental)
                    OR ciencia OR academico OR investigacion OR universidad OR profesor
                    OR postdoc OR postdoctoral OR PhD OR doctorado OR investigador
                    OR investigadora)
                    -is:retweet""",
    "environment": """location ((contaminacion del aire) OR contaminacion OR
                    (salud publica) OR (poor air) OR asma OR contaminado OR contaminada
                    OR smog OR (calidad del aire) OR (medio ambiente)) -is:retweet""",
}
