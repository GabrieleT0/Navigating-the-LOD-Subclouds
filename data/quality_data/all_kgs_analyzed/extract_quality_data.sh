rm *.csv
unzip quality_data.zip
for file in *.tar.gz; do
  tar -xzvf "$file" --wildcards --no-anchored '*.csv' -C ./ --strip-components=4
done

rm *.tar.gz