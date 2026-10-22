import math

def split_tickers_for_finviz(ticker_list, batch_size=25):
    """
    Splits a large list of tickers into safe batches for Finviz 
    and generates direct multi-ticker URLs.
    """
    # Clean up and uppercase all tickers
    cleaned_tickers = [t.strip().upper() for t in ticker_list if t.strip()]
    
    total_tickers = len(cleaned_tickers)
    num_batches = math.ceil(total_tickers / batch_size)
    
    print(f"Total Tickers: {total_tickers}")
    print(f"Batch Size: {batch_size}")
    print(f"Total Batches Generated: {num_batches}\n" + "="*50)
    
    batch_results = []
    
    for i in range(num_batches):
        start_idx = i * batch_size
        end_idx = min((i + 1) * batch_size, total_tickers)
        batch = cleaned_tickers[start_idx:end_idx]
        
        # Create comma-separated string for Finviz URL query
        ticker_string = ",".join(batch)
        
        # Finviz screener overview URL for custom ticker sets
        finviz_url = f"https://finviz.com/screener.ashx?v=111&t={ticker_string}"
        
        batch_info = {
            "batch_number": i + 1,
            "tickers": batch,
            "ticker_string": ticker_string,
            "url": finviz_url
        }
        batch_results.append(batch_info)
        
        # Print out batch details
        print(f"Batch {i + 1} ({len(batch)} tickers):")
        print(f"Tickers: {ticker_string}")
        print(f"Finviz URL: {finviz_url}\n")
        
    return batch_results

# --- Example Usage ---
# Simulate a large list (replace this with your 1,200 tickers from a CSV/file)
# e.g., mock_1200_tickers = ['AAPL', 'MSFT', 'NVDA', ...] 
mock_1200_tickers = [f"TICK{i}" for i in range(1, 1201)] 

# Run the function
finviz_batches = split_tickers_for_finviz(mock_1200_tickers, batch_size=30)