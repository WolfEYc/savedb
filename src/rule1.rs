use sqlx::{types::BigDecimal, FromRow, MySql, Pool};

const ZSCORE_THRESH: f32 = 2.5;
const PURCHASES_REQUIRED: i32 = 4;

#[derive(Debug, FromRow)]
pub struct Rule1Row {
    pub last_name: String,
    pub first_name: String,
    pub account_number: i32,
    pub purchase_number: i32,
    pub merchant_name: String,
    pub purchase_amount: BigDecimal,
    pub average: Option<BigDecimal>,
    pub std: Option<f64>,
    pub num: Option<i64>
}

pub async fn rule1rows(pool: &Pool<MySql>) -> Result<Vec<Rule1Row>, sqlx::Error> {
    sqlx::query_as!(Rule1Row,"
        SELECT a.last_name, a.first_name, p.account_number, p.purchase_number, p.merchant_name, p.purchase_amount, u.average, u.std, u.num
        FROM purchase p
        JOIN (
            SELECT
                account_number,
                merchant_number,
                AVG(purchase_amount) as average,
                STDDEV(purchase_amount) as std,
                COUNT(purchase_amount) as num
            FROM purchase
            GROUP BY account_number, merchant_number
        ) u ON p.account_number = u.account_number AND p.merchant_number = u.merchant_number
        JOIN account a ON p.account_number = a.account_number
        WHERE u.num > ? AND (ABS(p.purchase_amount - u.average) / u.std) > ?
        ORDER BY p.purchase_amount
    ",
    PURCHASES_REQUIRED,
    ZSCORE_THRESH
    )
    .fetch_all(pool)
    .await
}

#[cfg(test)]
mod tests {
    use prettytable::{Table, row};

    use crate::{connect_db, rule1::rule1rows};

    #[tokio::test]
    async fn print_rule1_rows() {
        dotenvy::dotenv().unwrap();
        let pool = connect_db().await.unwrap();
        let rows = rule1rows(&pool).await.unwrap();
        let mut table = Table::new();
        table.add_row(row!["amt", "avg", "num"]);

        for row in rows.iter().take(10) {
            table.add_row(row![row.purchase_amount.to_string(), row.average.clone().unwrap().to_string(), row.num.clone().unwrap()]);
        }

        table.printstd();
    }
}
