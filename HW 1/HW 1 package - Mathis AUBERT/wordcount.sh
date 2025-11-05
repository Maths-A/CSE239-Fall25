WORKDIR="$(pwd)"

# Create the txt folder to store downloaded wikipages
mkdir -p "$WORKDIR/txt"
cd "$WORKDIR"

# Download the wikipages if not already present
echo "Downloading enwik9.zip..."
curl -L -o enwik9.zip https://mattmahoney.net/dc/enwik9.zip


# Unzip the file
echo "Extracting enwik9.zip..."
unzip -oq enwik9.zip

# Move enwik9 to the txt folder
if [ -f "enwik9" ]; then
    mv enwik9 txt/enwik9.txt
else
    echo "Error: enwik9 file not found after unzip."
    exit 1
fi

# Remove enwik9.zip that is unnecessary now
rm -f enwik9.zip

# Run the wordcount.py file
echo "Running word count using Python..."
python3 wordcount.py
