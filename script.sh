source /home/luis/.virtualenvs/TFG_INFO2/bin/activate
python main.py -n one_week -p link_domain_combined -j one_week --inc_w 3600 --inc_n 168 -s seed_def.txt -m ./3_702010 > one_week_exit.txt
python main.py -n model_use -p link_domain_combined --inc_w 300 --inc_n 288 -s seed_def.txt -m ./3_702010 > model_use.txt
python main.py -n keyword_use -k -p link_domain_combined --inc_w 300 --inc_n 288 -s seed_def.txt -m ./3_702010 > keyword_use.txt
#python main.py -n 3_kfold_one_hour -p link_domain_combined --inc_w 15 --inc_n 240 -s seeds.txt -m ./3_kfold
#python main.py -n dehatebert -p link_domain_combined --inc_w 15 --inc_n 240 -s seeds.txt -m Hate-speech-CNERG/dehatebert-mono-spanish
#python main.py -n 3_kfold_one_hour_sur -p link_surroundings --inc_w 15 --inc_n 240 -s seeds.txt -m ./3_kfold
#python main.py -n dehatebert_sur -p link_surroundings --inc_w 15 --inc_n 240 -s seeds.txt -m Hate-speech-CNERG/dehatebert-mono-spanish
#python main.py -n 3_kfold_one_hour_un -p unvisited_domains --inc_w 15 --inc_n 240 -s seeds.txt -m ./3_kfold
#python main.py -n dehatebert_sur -p unvisited_domains --inc_w 15 --inc_n 240 -s seeds.txt -m Hate-speech-CNERG/dehatebert-mono-spanish
#python main.py -n link_surroundings_2_kfold -p link_surroundings --inc_w 15 --inc_n 20 -s seeds_hate.txt -m 2_kfold
#python main.py -n link_surroundings_1_kfold -p link_surroundings --inc_w 15 --inc_n 20 -s seeds_hate.txt -m 1_kfold
#python main.py -n link_surroundings_3_702010 -p link_surroundings --inc_w 15 --inc_n 20 -s seeds_hate.txt -m 3_702010
#python main.py -n link_surroundings_2_702010 -p link_surroundings --inc_w 15 --inc_n 20 -s seeds_hate.txt -m 2_702010
#python main.py -n link_surroundings_1_702010 -p link_surroundings --inc_w 15 --inc_n 20 -s seeds_hate.txt -m 1_702010
#python main.py -n link_surroundings_beto_v1 -p link_surroundings --inc_w 15 --inc_n 20 -s seeds_hate.txt -m beto_v1

