# PModel Cost Optimization Tool

`cost_model.py` provides a command-line workflow for selecting the lowest landed-cost procurement option for Great Plains (GP) purchasing lanes. The script can be used with live GP data over ODBC or with the built-in demo data set when a database connection is not configured.

## Mathematics

The optimization follows the exact landed-cost formulas embedded in `cost_model.py`:

- **Unit price curve** – vendor price decreases exponentially with quantity:
  \[
  p(Q) = p_{\min} + \left(p_{\max} - p_{\min}\right) e^{-\lambda Q}
  \]
  The procurement price component is \(p(Q) \times Q\).
- **Procurement base cost** – each order pays a fixed fee \(F_i\):
  \[
  C_{\text{proc}} = F_i + p(Q) Q
  \]
- **Freight shipments** – total tons \(w_i Q\) are divided by mode capacity \(\text{Cap}_m\):
  \[
  N_{\text{ship}} = \left\lceil \frac{w_i Q}{\text{Cap}_m} \right\rceil
  \]
- **Freight cost** – fixed per-shipment plus variable ton-mile charges:
  \[
  C_{\text{freight}} = N_{\text{ship}} F_m + c_m D_i w_i Q
  \]
- **Procurement tax** – optional tax applied to selected components (price, fixed order, fixed freight, variable freight):
  \[
  C_{\text{tax}} = \tau_i \left( \alpha_i p(Q)Q + \beta_i F_i + \gamma_i c_m D_i w_i Q + \delta_i N_{\text{ship}} F_m \right)
  \]

The total landed cost for a given (vendor, mode) option is the sum of these components:
\[
C_{\text{total}} = C_{\text{proc}} + C_{\text{freight}} + C_{\text{tax}}
\]

Every candidate lane/mode combination is evaluated with these equations; the tool chooses the minimum total cost.

## Features
- Enumerates every vendor/mode combination discovered in GP for a requested item and quantity.
- Applies procurement taxes exactly as described in the associated mathematics (price, fixed order, fixed freight, and variable freight components).
- Supports both single-item optimization and batch processing from CSV or Excel sheets.
- Falls back to an in-memory demo data set when database credentials are not supplied.

## Installation
1. Ensure Python 3.9+ is available.
2. (Optional) Install database and spreadsheet dependencies:
   ```bash
   pip install pyodbc pandas
   ```
3. Clone this repository and change into the project directory:
   ```bash
   git clone <repo-url>
   cd PModel
   ```

## Usage
Run the script with the `one` subcommand to optimize a single item:
```bash
python cost_model.py one --item NPK02020 --qty 250 --dsn GP_DSN
```

For batch processing using a CSV or Excel file containing `ItemCode` and `Quantity` columns:
```bash
python cost_model.py sheet --in items.csv --out plans.csv
```

### Database configuration
Provide ODBC connection details using the available flags:
```bash
python cost_model.py one --item NPK02020 --qty 250 \
  --driver "{ODBC Driver 17 for SQL Server}" \
  --server sql.host.local \
  --database GP \
  --username my_user \
  --password secret
```
If you omit these parameters, the script uses the built-in demo data. Include `--no-fallback` to require a live database connection.

## Output
The CLI prints a human-readable breakdown along with a JSON representation of the optimal plan. Batch mode writes a results file next to the input (or to the location specified by `--out`).

## License
This project is provided as-is; supply your preferred licensing text here.
