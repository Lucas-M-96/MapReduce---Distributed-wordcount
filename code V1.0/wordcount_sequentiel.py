import time
import os

path=os.path.dirname(os.path.realpath(__file__))


start_global=time.time()

start=time.time()
with open(path+'/input.txt') as f:
    text = f.read()
words = text.split()
duration_splt=time.time()-start


start=time.time()
dico = {}
for word in words:
    word = word.lower()
    dico[word] = dico.get(word, 0) + 1
duration_count=time.time()-start
    
# Tri en utilisant la fonction sorted de Python : retourne une liste de tuples
start=time.time()
res=sorted(dico.items(),key=lambda x : (-x[1],x[0]))
duration_sort=time.time()-start

start=time.time()
# sortie dans un fichier texte
with open(path+'/output_sequentiel.txt','w') as f:
    for t in res:
        f.write(f"{t[0]} {t[1]} \n")
duration_write=time.time()-start

duration_tot=time.time()-start_global

print(f"Duree séparation des mots : {round(duration_splt,2)}")
print(f"Duree comptage occurence : {round(duration_count,2)}")
print(f"Durée tri : {round(duration_sort,2)}")
print(f"Durée écriture : {round(duration_write,2)}")     

print(f'\nDurée totale : {round(duration_tot,2)}')
