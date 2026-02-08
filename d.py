import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# INPUT FILE (put all your URLs here)
INPUT_FILE = "links.txt"

# OUTPUT FILE
OUTPUT_FILE = "download_links.txt"

headers = {
    "User-Agent": "Mozilla/5.0"
}

def get_download_link(url):
    try:
        print(f"Opening: {url}")

        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        # Find DOWNLOAD button
        # On your screenshot it is an <a> tag with text "DOWNLOAD"
        for a in soup.find_all("a"):
            if a.text.strip().upper() == "DOWNLOAD":
                download_url = urljoin(url, a.get("href"))
                return download_url

        print("Download button not found.")
        return None

    except Exception as e:
        print("Error:", e)
        return None


def main():
    with open(INPUT_FILE, "r") as f:
        urls = [line.strip() for line in f if line.strip()]

    results = []

    for u in urls:
        link = get_download_link(u)
        if link:
            results.append(link)

    # Save output
    with open(OUTPUT_FILE, "w") as f:
        for r in results:
            f.write(r + "\n")

    print(f"\nSaved {len(results)} links to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
