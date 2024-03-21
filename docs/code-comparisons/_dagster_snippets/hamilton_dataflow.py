from hamilton.function_modifiers import extract_columns

NEWSTORIES_URL = "https://hacker-news.firebaseio.com/v0/topstories.json"

def topstory_ids(newstories_url: str = NEWSTORIES_URL) -> list[int]:
    """Query the id of the top HackerNews stories"""
    return requests.get(newstories_url).json()[:100]

@extract_columns("title")
def topstories(topstory_ids: list[int]) -> pd.DataFrame:
    """Query the top HackerNews stories based on ids"""
    results = []
    for item_id in topstory_ids:
        item = requests.get(
            f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json"
        ).json()
        results.append(item)
    return pd.DataFrame(results)

def most_frequent_words(title: pd.Series) -> dict[str, int]:
    """Compute word frequency in HackerNews story titles"""
    STOPWORDS = ["a", "the", "an", "of", "to", "in",
                 "for", "and", "with", "on", "is", "\u2013"]
    word_counts = {}
    for raw_title in title:
        for word in raw_title.lower().split():
            word = word.strip(".,-!?:;()[]'\"-")
            if len(word) == 0:
                continue
            
            if word in STOPWORDS:
                continue
            
            word_counts[word] = word_counts.get(word, 0) + 1     
    return word_counts
                 
def top_25_words_plot(most_frequent_words: dict[str, int]) -> Figure:
    """Bar plot of the frequency of the top 25 words in HackerNews titles"""
    top_words = {
        pair[0]: pair[1]
        for pair in sorted(
            most_frequent_words.items(), key=lambda x: x[1], reverse=True
        )[:25]
    }
    
    fig = plt.figure(figsize=(10, 6))
    plt.bar(list(top_words.keys()), list(top_words.values()))
    plt.xticks(rotation=45, ha="right")
    plt.title("Top 25 Words in Hacker News Titles")
    plt.tight_layout()
    return fig

@extract_columns("registered_at")
def signups(hackernews_api: DataGeneratorResource) -> pd.DataFrame:
    """Query HackerNews signups using a mock API endpoint"""
    return pd.DataFrame(hackernews_api.get_signups())

def earliest_signup(registered_at: pd.Series) -> int:
    return registered_at.min()

def latest_signup(registered_at: pd.Series) -> int:
    return registered_at.min()
