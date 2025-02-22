docker run -v "//c/Users/aidan/Downloads/Lab_6_export/word-count:/tmp/wc-demo" -it \
    -p 8888:8888 \
    --name wc-container \
    pyspark-image