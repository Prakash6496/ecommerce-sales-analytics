from load_data import run_load
from report import generate_report
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
import logging

# Setup logging
logging.basicConfig(
    filename="pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def run_pipeline():
    print(f"\n🚀 Pipeline started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)
    try:
        # Step 1 - Load fresh data
        run_load()
        logging.info("Data loaded successfully")

        # Step 2 - Generate report
        generate_report()
        logging.info("Report generated successfully")

        print("=" * 50)
        print(f"🎉 Pipeline completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info("Pipeline completed successfully")

    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        logging.error(f"Pipeline failed: {e}")

def run_scheduler():
    scheduler = BlockingScheduler()

    # Run every Monday at 9:00 AM
    @scheduler.scheduled_job("cron", day_of_week="mon", hour=9, minute=0)
    def scheduled_job():
        print("⏰ Weekly scheduler triggered!")
        run_pipeline()

    print("🕐 Scheduler started — pipeline runs every Monday at 9:00 AM")
    print("   Press Ctrl+C to stop")
    scheduler.start()

if __name__ == "__main__":
    # Run pipeline once immediately
    run_pipeline()