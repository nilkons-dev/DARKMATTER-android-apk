# DARKMATTER – Android app

## APK বানানোর নিয়ম (GitHub দিয়ে, কম্পিউটারে কিছু install লাগবে না)
1. github.com-এ নতুন একটি **private** repository খুলুন।
2. এই ZIP-এর সব ফাইল ও ফোল্ডার repository-তে আপলোড করুন। **`.github` ফোল্ডারটাও** আপলোড করতে হবে
   (hidden ফোল্ডার, না দিলে APK বিল্ড শুরুই হবে না)।
3. Repository-র **Actions** ট্যাবে যান। "Build APK" নিজে চলবে (না চললে "Run workflow" চাপুন)। প্রায় ৫–১০ মিনিট লাগে।
4. শেষ হলে ওই run খুলে নিচে **Artifacts → DARKMATTER-apk** ডাউনলোড করুন, ZIP খুলে `app-debug.apk` ফোনে install করুন
   ("Unknown sources" চালু করতে হবে)।

বিল্ড ফেল করলে Actions-এর লাল step-এর লগ আমাকে পাঠান, ঠিক করে দেব।

## প্রথমবার ব্যবহার
1. **Settings → Quotex account → Log in to Quotex**: লগইন করুন (Demo/Live এখানেই বেছে নিন), তারপর Done।
2. **Settings → Telegram**: token ও chat ID দিন → **Test Telegram** ও **Test chart image** চাপুন।
   Test chart image-এ ছবি দেখা গেলে সিগন্যালের চার্টও যাবে।
3. **Home → Connect** → **Markets** থেকে মার্কেট বাছুন → **Start signals**।
4. স্ক্রিন বন্ধ থাকলেও চালাতে **Settings → App → Allow background** চাপুন। কাজ শেষে **Exit app**।

## ফাইল
- `app/src/main/python/darkmatter_bot.py` – আপনার আসল বট, **একটা অক্ষরও বদলানো হয়নি**
- `app/src/main/python/dm_app.py` – অ্যাপের ব্যাকএন্ড (বটকে বাইরে থেকে চালায়)
- `app/src/main/python/dm_ui/` – অ্যাপের UI (লাইট/ডার্ক সহ)
- `app/src/main/java/...` – Android অংশ (ছোট)
- `tests/run_integration.py` – ফোন ছাড়াই পুরো প্রবাহের টেস্ট: `python3 tests/run_integration.py` (Pillow লাগে)
