import re
import time
from ttp import ttp

#Fecha del log
file_name = '2024-09-20'


""" 
    ------- Pre-procesado -------
    En esta etapa se identifican el header y las variables dinámicas triviales, eliminando el primero y
    marcando los segundos.

    Retorna: 
        - processed_lines: str[] -> Lista de lineas de log procesadas.
"""
processed_lines = [] 

with open(file='logs/wt1tcs.{0}.log'.format(file_name), mode='r', encoding='iso-8859-1') as logs:

    lines = logs.readlines()

    for line in lines:
        
        #Eliminación del header del log
        header_re = re.compile(r'([a-zA-Z]{3}\s[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}?)\swt1tcs\s(.+)(\[[0-9]+\]):')

        headerless_line = re.sub(header_re, '', line)

        #Eliminación de variables dinámicas triviales        
        datetime_re = re.compile(r'[0-2][0-9]{3}-[0-1][0-9]-[0-3][0-9]T[0-2][0-9]:[0-5][0-9]:[0-5][0-9](.[0-9]+)*')
        date_re = re.compile(r'[0-2][0-9]{3}-[0-1][0-9]-[0-3][0-9]')
        time_re = re.compile(r'[0-2][0-9]:[0-5][0-9]:[0-5][0-9](.[0-9]+)*')


        datetimeless_line = re.sub(datetime_re, '{{datetime}}', headerless_line)
        dateless_line = re.sub(date_re, '{{date}}', datetimeless_line)
        timeless_line = re.sub(time_re, '{{time}}', dateless_line)

        processed_lines.append(timeless_line)

""" 
    ------- Agrupamiento -------
    En esta etapa se delimitan los tokens de cada linea, se presumen los tokens estáticos como aquellos que solo posean letras
    y se agrupan las lineas según estos tokens estáticos.

    Retorna: 
        - grouped_lines: dict[] -> Lista de diccionarios, cada diccionario compuesto de dos campos:
            * s_t: Lista con tokens estáticos.
            * lines: Lista con los tokens de las lineas de log correspondientes al grupo.
"""
grouped_lines = []

for line in processed_lines:

    #Delimitar tokens
    tokens = line.split()

    #Identificar tokens estáticos
    static_tokens = [x for x in tokens if x.isalpha()]

    #Comparar tokens estáticos con otros grupos; guardar si hay match
    saved_flag = False

    for group in grouped_lines:
        if (group["s_t"] == static_tokens):
            group["lines"].append(tokens)
            saved_flag = True
            break

    #Si no hay match, crear y guardar en nuevo grupo
    if (not saved_flag):
        new_group = {
            "s_t" : static_tokens,
            "lines" : [tokens]
        }

        grouped_lines.append(new_group)

""" 
    ------- Análisis -------
    En esta etapa se cuenta la repetición de tokens por grupo y luego se dividen entre tokens estáticos y tokens dinámicos
    basandose en el número de veces que se repiten: aquellos que se repitan en todos los tokens del grupo se consideran estáticos y se
    añaden a una plantilla, y el resto de tokens se consideran dinámicos y se extraen como datos del grupo.

    Retorna:
        - templates: str[] -> Lista de las plantillas de cada grupo.
"""
templates = []

for group in grouped_lines:
    
    #Ordenar lineas del grupo segun tamaño de lista de forma descendiente
    group["lines"].sort(key=lambda x: len(x), reverse=True)

    #Obtener máximo de las lineas del grupo
    max_length = len(group["lines"][0])

    #Contar repeticiones de tokens
    token_counter = {}

    for tokens in group["lines"]:

        #Obtener duplicados
        duplicates = [x for x in set(tokens) if tokens.count(x) > 1]

        #Conteo
        for token in tokens:
            if(token in list(token_counter) and token not in duplicates):
                token_counter[token] += 1
            
            else:
                if(token not in duplicates):
                    token_counter[token] = 1
                else:
                    token_counter[token + '_duplicado'] = 1
                    duplicates.remove(token)

    #Selección de tokens para plantilla segun repeticiones
    template = ""

    #Obtener la máxima frecuencia de ocurrencia de tokens
    max_frecuency = len(group["lines"])

    #Contador de tokens dinámicos
    i_td = 0

    #Contador de tokens totales
    i_tt = 0

    #Generación de la plantilla
    while (i_tt < max_length):

        try:
            token = list(token_counter)[i_tt]

            if (token_counter[token] == max_frecuency):
                template = template + " " + token + " "

            else:
                template = template + " {{%d}} " % i_td
                i_td += 1

            i_tt += 1
        
        except IndexError as e:
            #print(template)
            break
    
    #Normalizar duplicados
    template_final = template.replace('_duplicado', '')

    #Guardar plantilla y datos
    templates.append(template_final)

""" 
    ------ Parsing -------
    Se parsea cada linea de log usando las plantillas generada anteriormente, usando TTP.
"""
parsed_data = []

i_time = time.time()

for line in processed_lines:
    for template in templates:
        try:
            parser = ttp(data=line, template=template)
            parser.parse()
            result = parser.result(template='per_input', format="dictionary")[0][0]

            if(len(result) != 0): 
                parsed_data.append(result)
                break

        except re.PatternError as e:
            continue

f_time = time.time()

print(f_time - i_time)