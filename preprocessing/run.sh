declare -a arr=("xaa") #xab xac xad xae xaf
for chunk in "${arr[@]}";
do
	echo "processing $chunk"
	python main.py -if ${chunk}Part_EducationPakistan_201301 -of Eng_${chunk}_Part_EducationPakistan_201301.tsv
done
