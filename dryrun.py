import agent

con = agent.db_init()
arts = agent.fetch_rss("https://feeds.bbci.co.uk/news/world/rss.xml")
print("RSS raw:", len(arts))

kept = []
for a in arts:
    t, u = a["title"], a["url"]
    if any(k in t.lower() for k in agent.CATEGORIES["Wars & Conflict"]["must_match"]):
        if agent.is_new(con, u, t):
            kept.append({"title": t, "url": u, "domain": a["domain"],
                         "lang": "English", "country": ""})
print("after filter:", len(kept))

picked = agent.gemini_batch("Wars & Conflict", kept[:10])
print("after Gemini:", len(picked))
for p in picked:
    print(" -", p["impact"], p["title_en"][:60])

agent.deliver("Wars & Conflict", picked, 5)
print("sent to topic 5")