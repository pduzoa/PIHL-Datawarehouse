
# run_all_periods.py
import logging
import sys
from datetime import datetime

from full_extractor import FullExtractor
from periods import generate_periods

# ===================================================================== #
# CONFIGURATION
# ===================================================================== #
# Set the exact years you want to extract. 
# Based on your datasets, most are 2015-2026. Let's grab 2023-2024 for now.
START_YEAR = 2017          
END_YEAR = 2026            
END_MONTH = 12             

# We focus on Monthly (the majority of your data).
# We add Quarterly just to catch the one WHO DR TB dataset.
PERIOD_TYPES = ["Monthly", "Quarterly"] 

BATCH_SIZE = 10            
DELAY = 0.3                

# ===================================================================== #
# LOGGING
# ===================================================================== #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/run_all_periods.log", mode="a", encoding="utf-8"),
    ],
)
logger = logging.getLogger("run_all_periods")

def run():
    stats = {
        "attempted": 0,
        "success": 0,
        "failed": 0,
        "start_time": datetime.utcnow()
    }

    logger.info("=" * 70)
    logger.info(f"STARTING FULL EXTRACTION: {START_YEAR} to {END_YEAR}")
    logger.info(f"Period Types: {', '.join(PERIOD_TYPES)}")
    logger.info("=" * 70)

    for p_type in PERIOD_TYPES:
        periods = generate_periods(p_type, START_YEAR, END_YEAR, END_MONTH)
        logger.info(f"\n>>> PROCESSING {p_type.upper()} ({len(periods)} periods)")

        for i, period in enumerate(periods, 1):
            stats["attempted"] += 1
            logger.info(f"[{i}/{len(periods)}] Period: {period}")

            try:
                # Initialize extractor for this specific period
                fe = FullExtractor(
                    period=period,
                    batch_size=BATCH_SIZE,
                    delay=DELAY,
                    period_type_filter=p_type
                )
                
                # Run it (checkpoints will prevent re-processing if it crashes)
                fe.run()
                stats["success"] += 1

            except KeyboardInterrupt:
                logger.warning("\nInterrupted by user. Stopping gracefully.")
                sys.exit(130)
            except Exception as e:
                stats["failed"] += 1
                logger.error(f"FAILED period {period} ({p_type}): {e}", exc_info=True)

    # Final Summary
    elapsed = (datetime.utcnow() - stats["start_time"]).total_seconds()
    logger.info("\n" + "=" * 70)
    logger.info("EXTRACTION COMPLETE")
    logger.info(f"Periods Attempted: {stats['attempted']}")
    logger.info(f"Periods Success:   {stats['success']}")
    logger.info(f"Periods Failed:    {stats['failed']}")
    logger.info(f"Total Time:        {elapsed/60:.1f} minutes")
    logger.info("=" * 70)

if __name__ == "__main__":
    run()
