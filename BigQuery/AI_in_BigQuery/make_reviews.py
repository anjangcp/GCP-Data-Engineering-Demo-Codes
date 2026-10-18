"""Generate the made-up customer reviews used in the AI in BigQuery demo.

Run:  python3 make_reviews.py   ->  reviews.jsonl (one JSON object per line)

The reviews are fictional. They are written so the five demo queries have something to find:
about a third are complaints, product quality is the largest complaint topic, and complaints
become more frequent over the 90 days so a forecast has a trend to pick up. The seed is fixed,
so the file is the same every time.
"""
import json
import random
from datetime import date, timedelta

random.seed(11)
START, DAYS, COUNT = date(2026, 7, 6), 90, 400
PRODUCTS = ["Coffee machine", "Electric kettle", "Blender", "Toaster", "Air fryer"]

GOOD = [
    "Really happy with this {p}. It works exactly as described and looks great in my kitchen.",
    "The {p} arrived early and was well packed. I have used it every day for {d} days with no trouble.",
    "Good value for the price. The {p} does what it should and is easy to clean.",
    "Bought the {p} as a gift and they love it. Would buy again.",
    "Solid build quality. I was unsure at first but the {p} has been excellent for {d} days now.",
    "Setup took two minutes. The {p} is quiet and quick. Five stars.",
    "Does the job. Nothing fancy, but the {p} is reliable and the instructions were clear.",
    "I had a question before ordering and support answered within an hour. The {p} itself is great.",
    "Better than my old one. The {p} heats up fast and feels sturdy.",
]
QUALITY = [
    "The {p} started leaking from the base after {d} days. Water all over the counter. I want a refund.",
    "After only {d} days the {p} stopped working completely. It will not switch on. Please replace it.",
    "The lid of the {p} cracked after {d} days of normal use. Very poor quality. I would like my money back.",
    "My {p} makes a loud grinding noise and smells of burning after {d} days. This feels unsafe. Refund please.",
    "The handle came off the {p} on day {d}. I expected it to last longer than that. Can I get a replacement?",
    "The {p} trips the electricity every time I turn it on. It did this from day {d}. I am returning it.",
    "Buttons on the {p} stopped responding after {d} days. Disappointed, but I will keep it if you can send a new panel.",
    "The {p} leaks a little from the side seal. It started around day {d}. Not a disaster but annoying.",
]
DELIVERY = [
    "The {p} took three weeks to arrive and the tracking never updated. Not acceptable.",
    "My {p} arrived with the box crushed and a dent on the side. The courier just left it outside.",
    "Delivery of the {p} was a week late and nobody told me. I had to chase it myself.",
    "Wrong item delivered. I ordered a {p} and received something else. Still waiting for the right one.",
]
PRICE = [
    "The {p} went on sale for much less two days after I bought it. I feel overcharged.",
    "Too expensive for what it is. The {p} is fine but not worth this price.",
    "I was charged twice for the {p} and I am still waiting for the second charge to be returned.",
]
SERVICE = [
    "I have emailed support three times about my {p} and had no reply. Terrible service.",
    "Support was rude when I asked about the warranty on my {p}. I will not order here again.",
    "The chat agent closed my conversation about the {p} without answering. Very frustrating.",
]
COMPLAINTS = [(QUALITY, 0.52), (DELIVERY, 0.22), (PRICE, 0.11), (SERVICE, 0.15)]


def main():
    rows = []
    for i in range(1, COUNT + 1):
        day = int(DAYS * random.random() ** 0.8)                     # a little more volume later on
        complaint_chance = 0.18 + 0.30 * day / DAYS                   # complaints rise over time
        product = random.choices(PRODUCTS, weights=[5, 2, 2, 2, 2])[0]
        if random.random() < complaint_chance:
            pool = random.choices([c for c, _ in COMPLAINTS], weights=[w for _, w in COMPLAINTS])[0]
        else:
            pool = GOOD
        text = random.choice(pool).format(p=product.lower(), d=random.randint(3, 40))
        rows.append({"review_id": i, "product": product,
                     "review_date": str(START + timedelta(days=day)), "review_text": text})
    rows.sort(key=lambda r: (r["review_date"], r["review_id"]))
    with open("reviews.jsonl", "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    print(f"{len(rows)} reviews, {rows[0]['review_date']} to {rows[-1]['review_date']} -> reviews.jsonl")


if __name__ == "__main__":
    main()
