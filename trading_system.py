import sqlite3
import json
import random
import time
import threading
from datetime import datetime, timedelta
from flask import Flask, request, jsonify
from flask_restx import Api, Resource, fields, Namespace
import uuid
from concurrent.futures import ThreadPoolExecutor
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_path='trading_system.db'):
        self.db_path = db_path
        self.init_database()
    
    def get_connection(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn
    
    def init_database(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Users table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                balance REAL DEFAULT 10000.0,
                loan_amount REAL DEFAULT 0.0,
                max_loan REAL DEFAULT 100000.0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Stocks table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stocks (
                id TEXT PRIMARY KEY,
                symbol TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                current_price REAL NOT NULL,
                available_quantity INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Stock price history
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_price_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_id TEXT NOT NULL,
                price REAL NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (stock_id) REFERENCES stocks (id)
            )
        ''')
        
        # User portfolios
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_portfolios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                stock_id TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                avg_buy_price REAL NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users (id),
                FOREIGN KEY (stock_id) REFERENCES stocks (id),
                UNIQUE(user_id, stock_id)
            )
        ''')
        
        # Transactions
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                stock_id TEXT NOT NULL,
                transaction_type TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                price REAL NOT NULL,
                total_amount REAL NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (id),
                FOREIGN KEY (stock_id) REFERENCES stocks (id)
            )
        ''')
        
        # Loans
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS loans (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                amount REAL NOT NULL,
                interest_rate REAL DEFAULT 0.05,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (id)
            )
        ''')
        
        conn.commit()
        conn.close()

class StockTradingSystem:
    def __init__(self):
        self.db = DatabaseManager()
        self.price_update_thread = None
        self.stop_price_updates = False
        self.setup_initial_data()
        
    def setup_initial_data(self):
        """Initialize system with sample stocks and users"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        # Check if data already exists
        cursor.execute("SELECT COUNT(*) FROM stocks")
        if cursor.fetchone()[0] == 0:
            # Add sample stocks
            sample_stocks = [
                ('AAPL', 'Apple Inc.', 150.0, 1000),
                ('GOOGL', 'Alphabet Inc.', 80.0, 800),
                ('MSFT', 'Microsoft Corp.', 90.0, 900),
                ('TSLA', 'Tesla Inc.', 75.0, 600),
                ('AMZN', 'Amazon.com Inc.', 85.0, 700)
            ]
            
            for symbol, name, price, quantity in sample_stocks:
                stock_id = str(uuid.uuid4())
                cursor.execute('''
                    INSERT INTO stocks (id, symbol, name, current_price, available_quantity)
                    VALUES (?, ?, ?, ?, ?)
                ''', (stock_id, symbol, name, price, quantity))
                
                # Add initial price history
                cursor.execute('''
                    INSERT INTO stock_price_history (stock_id, price)
                    VALUES (?, ?)
                ''', (stock_id, price))
        
        # Check if test users exist
        cursor.execute("SELECT COUNT(*) FROM users")
        if cursor.fetchone()[0] == 0:
            # Add test users
            for i in range(10):
                user_id = str(uuid.uuid4())
                username = f"user_{i+1}"
                cursor.execute('''
                    INSERT INTO users (id, username, balance)
                    VALUES (?, ?, ?)
                ''', (user_id, username, 10000.0))
        
        conn.commit()
        conn.close()
    
    def start_price_updates(self):
        """Start background thread for price updates"""
        if self.price_update_thread is None or not self.price_update_thread.is_alive():
            self.stop_price_updates = False
            self.price_update_thread = threading.Thread(target=self._update_prices_periodically)
            self.price_update_thread.daemon = True
            self.price_update_thread.start()
            logger.info("Started price update thread")
    
    def stop_price_update_thread(self):
        """Stop the price update thread"""
        self.stop_price_updates = True
        if self.price_update_thread:
            self.price_update_thread.join()
    
    def _update_prices_periodically(self):
        """Background function to update stock prices every 5 minutes"""
        while not self.stop_price_updates:
            try:
                self.update_stock_prices()
                logger.info("Updated stock prices")
                time.sleep(300)  # 5 minutes
            except Exception as e:
                logger.error(f"Error updating prices: {str(e)}")
                time.sleep(60)  # Wait 1 minute before retrying
    
    def update_stock_prices(self):
        """Update all stock prices randomly within 1-100 range"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT id, current_price FROM stocks")
        stocks = cursor.fetchall()
        
        for stock in stocks:
            stock_id, current_price = stock['id'], stock['current_price']
            
            # Generate new price with some volatility
            change_percent = random.uniform(-0.1, 0.1)  # ±10% change
            new_price = current_price * (1 + change_percent)
            
            # Ensure price stays between 1 and 100
            new_price = max(1.0, min(100.0, new_price))
            
            # Update current price
            cursor.execute('''
                UPDATE stocks SET current_price = ? WHERE id = ?
            ''', (new_price, stock_id))
            
            # Add to price history
            cursor.execute('''
                INSERT INTO stock_price_history (stock_id, price)
                VALUES (?, ?)
            ''', (stock_id, new_price))
        
        conn.commit()
        conn.close()
    
    def register_stock(self, symbol, name, price, quantity):
        """Register a new stock"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        try:
            stock_id = str(uuid.uuid4())
            price = max(1.0, min(100.0, price))  # Ensure price is between 1-100
            
            cursor.execute('''
                INSERT INTO stocks (id, symbol, name, current_price, available_quantity)
                VALUES (?, ?, ?, ?, ?)
            ''', (stock_id, symbol.upper(), name, price, quantity))
            
            # Add initial price history
            cursor.execute('''
                INSERT INTO stock_price_history (stock_id, price)
                VALUES (?, ?)
            ''', (stock_id, price))
            
            conn.commit()
            return {"success": True, "stock_id": stock_id, "message": "Stock registered successfully"}
        except sqlite3.IntegrityError:
            return {"success": False, "message": "Stock symbol already exists"}
        finally:
            conn.close()
    
    def get_stock_history(self, symbol=None):
        """Get stock price history"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        if symbol:
            cursor.execute('''
                SELECT s.symbol, s.name, sph.price, sph.timestamp
                FROM stock_price_history sph
                JOIN stocks s ON sph.stock_id = s.id
                WHERE s.symbol = ?
                ORDER BY sph.timestamp DESC
                LIMIT 100
            ''', (symbol.upper(),))
        else:
            cursor.execute('''
                SELECT s.symbol, s.name, sph.price, sph.timestamp
                FROM stock_price_history sph
                JOIN stocks s ON sph.stock_id = s.id
                ORDER BY sph.timestamp DESC
                LIMIT 500
            ''')
        
        history = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return history
    
    def take_loan(self, user_id, amount):
        """Allow user to take a loan"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        # Check user eligibility
        cursor.execute("SELECT balance, loan_amount, max_loan FROM users WHERE id = ?", (user_id,))
        user = cursor.fetchone()
        
        if not user:
            conn.close()
            return {"success": False, "message": "User not found"}
        
        current_loan = user['loan_amount']
        max_loan = user['max_loan']
        
        if current_loan + amount > max_loan:
            conn.close()
            return {"success": False, "message": f"Loan amount exceeds maximum limit of {max_loan}"}
        
        # Process loan
        loan_id = str(uuid.uuid4())
        new_balance = user['balance'] + amount
        new_loan_amount = current_loan + amount
        
        cursor.execute('''
            INSERT INTO loans (id, user_id, amount)
            VALUES (?, ?, ?)
        ''', (loan_id, user_id, amount))
        
        cursor.execute('''
            UPDATE users SET balance = ?, loan_amount = ?
            WHERE id = ?
        ''', (new_balance, new_loan_amount, user_id))
        
        conn.commit()
        conn.close()
        
        return {
            "success": True,
            "loan_id": loan_id,
            "amount": amount,
            "new_balance": new_balance,
            "total_loan": new_loan_amount
        }
    
    def buy_stock(self, user_id, symbol, quantity):
        """Buy stocks for a user"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        try:
            # Get stock info
            cursor.execute('''
                SELECT id, current_price, available_quantity
                FROM stocks WHERE symbol = ?
            ''', (symbol.upper(),))
            stock = cursor.fetchone()
            
            if not stock:
                return {"success": False, "message": "Stock not found"}
            
            stock_id, price, available = stock['id'], stock['current_price'], stock['available_quantity']
            
            if quantity > available:
                return {"success": False, "message": "Insufficient stock availability"}
            
            # Get user info
            cursor.execute("SELECT balance FROM users WHERE id = ?", (user_id,))
            user = cursor.fetchone()
            
            if not user:
                return {"success": False, "message": "User not found"}
            
            total_cost = price * quantity
            
            if user['balance'] < total_cost:
                return {"success": False, "message": "Insufficient balance"}
            
            # Process transaction
            transaction_id = str(uuid.uuid4())
            new_balance = user['balance'] - total_cost
            new_available = available - quantity
            
            # Update user balance
            cursor.execute('''
                UPDATE users SET balance = ? WHERE id = ?
            ''', (new_balance, user_id))
            
            # Update stock availability
            cursor.execute('''
                UPDATE stocks SET available_quantity = ? WHERE id = ?
            ''', (new_available, stock_id))
            
            # Update user portfolio
            cursor.execute('''
                SELECT quantity, avg_buy_price FROM user_portfolios
                WHERE user_id = ? AND stock_id = ?
            ''', (user_id, stock_id))
            
            portfolio = cursor.fetchone()
            
            if portfolio:
                # Update existing position
                old_qty, old_avg = portfolio['quantity'], portfolio['avg_buy_price']
                new_qty = old_qty + quantity
                new_avg = ((old_qty * old_avg) + (quantity * price)) / new_qty
                
                cursor.execute('''
                    UPDATE user_portfolios SET quantity = ?, avg_buy_price = ?
                    WHERE user_id = ? AND stock_id = ?
                ''', (new_qty, new_avg, user_id, stock_id))
            else:
                # Create new position
                cursor.execute('''
                    INSERT INTO user_portfolios (user_id, stock_id, quantity, avg_buy_price)
                    VALUES (?, ?, ?, ?)
                ''', (user_id, stock_id, quantity, price))
            
            # Record transaction
            cursor.execute('''
                INSERT INTO transactions (id, user_id, stock_id, transaction_type, quantity, price, total_amount)
                VALUES (?, ?, ?, 'BUY', ?, ?, ?)
            ''', (transaction_id, user_id, stock_id, quantity, price, total_cost))
            
            conn.commit()
            
            return {
                "success": True,
                "transaction_id": transaction_id,
                "symbol": symbol.upper(),
                "quantity": quantity,
                "price": price,
                "total_cost": total_cost,
                "new_balance": new_balance
            }
            
        except Exception as e:
            conn.rollback()
            return {"success": False, "message": str(e)}
        finally:
            conn.close()
    
    def sell_stock(self, user_id, symbol, quantity):
        """Sell stocks for a user"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        try:
            # Get stock info
            cursor.execute('''
                SELECT id, current_price, available_quantity
                FROM stocks WHERE symbol = ?
            ''', (symbol.upper(),))
            stock = cursor.fetchone()
            
            if not stock:
                return {"success": False, "message": "Stock not found"}
            
            stock_id, price, available = stock['id'], stock['current_price'], stock['available_quantity']
            
            # Check user portfolio
            cursor.execute('''
                SELECT quantity, avg_buy_price FROM user_portfolios
                WHERE user_id = ? AND stock_id = ?
            ''', (user_id, stock_id))
            
            portfolio = cursor.fetchone()
            
            if not portfolio or portfolio['quantity'] < quantity:
                return {"success": False, "message": "Insufficient stocks to sell"}
            
            # Get user info
            cursor.execute("SELECT balance FROM users WHERE id = ?", (user_id,))
            user = cursor.fetchone()
            
            # Process transaction
            transaction_id = str(uuid.uuid4())
            total_earnings = price * quantity
            new_balance = user['balance'] + total_earnings
            new_portfolio_qty = portfolio['quantity'] - quantity
            new_available = available + quantity
            
            # Update user balance
            cursor.execute('''
                UPDATE users SET balance = ? WHERE id = ?
            ''', (new_balance, user_id))
            
            # Update stock availability
            cursor.execute('''
                UPDATE stocks SET available_quantity = ? WHERE id = ?
            ''', (new_available, stock_id))
            
            # Update portfolio
            if new_portfolio_qty == 0:
                cursor.execute('''
                    DELETE FROM user_portfolios WHERE user_id = ? AND stock_id = ?
                ''', (user_id, stock_id))
            else:
                cursor.execute('''
                    UPDATE user_portfolios SET quantity = ? WHERE user_id = ? AND stock_id = ?
                ''', (new_portfolio_qty, user_id, stock_id))
            
            # Record transaction
            cursor.execute('''
                INSERT INTO transactions (id, user_id, stock_id, transaction_type, quantity, price, total_amount)
                VALUES (?, ?, ?, 'SELL', ?, ?, ?)
            ''', (transaction_id, user_id, stock_id, quantity, price, total_earnings))
            
            conn.commit()
            
            return {
                "success": True,
                "transaction_id": transaction_id,
                "symbol": symbol.upper(),
                "quantity": quantity,
                "price": price,
                "total_earnings": total_earnings,
                "new_balance": new_balance
            }
            
        except Exception as e:
            conn.rollback()
            return {"success": False, "message": str(e)}
        finally:
            conn.close()
    
    def get_user_report(self, user_id):
        """Generate user performance report"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        # Get user basic info
        cursor.execute('''
            SELECT username, balance, loan_amount FROM users WHERE id = ?
        ''', (user_id,))
        user = cursor.fetchone()
        
        if not user:
            conn.close()
            return {"success": False, "message": "User not found"}
        
        # Get portfolio value
        cursor.execute('''
            SELECT up.quantity, up.avg_buy_price, s.current_price, s.symbol
            FROM user_portfolios up
            JOIN stocks s ON up.stock_id = s.id
            WHERE up.user_id = ?
        ''', (user_id,))
        
        portfolio = cursor.fetchall()
        portfolio_value = 0
        unrealized_pnl = 0
        
        portfolio_details = []
        for holding in portfolio:
            qty = holding['quantity']
            avg_price = holding['avg_buy_price']
            current_price = holding['current_price']
            symbol = holding['symbol']
            
            position_value = qty * current_price
            position_pnl = qty * (current_price - avg_price)
            
            portfolio_value += position_value
            unrealized_pnl += position_pnl
            
            portfolio_details.append({
                "symbol": symbol,
                "quantity": qty,
                "avg_buy_price": avg_price,
                "current_price": current_price,
                "position_value": position_value,
                "unrealized_pnl": position_pnl
            })
        
        # Get realized PnL from transactions
        cursor.execute('''
            SELECT 
                SUM(CASE WHEN transaction_type = 'SELL' THEN total_amount ELSE 0 END) as total_sales,
                SUM(CASE WHEN transaction_type = 'BUY' THEN total_amount ELSE 0 END) as total_purchases
            FROM transactions WHERE user_id = ?
        ''', (user_id,))
        
        txn_summary = cursor.fetchone()
        total_sales = txn_summary['total_sales'] or 0
        total_purchases = txn_summary['total_purchases'] or 0
        
        # Calculate net worth
        initial_balance = 10000.0  # Starting balance
        net_worth = user['balance'] + portfolio_value - user['loan_amount']
        total_pnl = net_worth - initial_balance
        
        conn.close()
        
        return {
            "success": True,
            "user_info": {
                "username": user['username'],
                "balance": user['balance'],
                "loan_amount": user['loan_amount'],
                "portfolio_value": portfolio_value,
                "net_worth": net_worth
            },
            "performance": {
                "total_pnl": total_pnl,
                "unrealized_pnl": unrealized_pnl,
                "realized_pnl": total_sales - total_purchases,
                "total_purchases": total_purchases,
                "total_sales": total_sales
            },
            "portfolio": portfolio_details
        }
    
    def get_stock_report(self):
        """Generate stock performance report"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT 
                s.symbol,
                s.name,
                s.current_price,
                s.available_quantity,
                COUNT(t.id) as transaction_count,
                SUM(CASE WHEN t.transaction_type = 'BUY' THEN t.quantity ELSE 0 END) as total_bought,
                SUM(CASE WHEN t.transaction_type = 'SELL' THEN t.quantity ELSE 0 END) as total_sold,
                AVG(t.price) as avg_transaction_price,
                MAX(sph.price) as max_price,
                MIN(sph.price) as min_price
            FROM stocks s
            LEFT JOIN transactions t ON s.id = t.stock_id
            LEFT JOIN stock_price_history sph ON s.id = sph.stock_id
            GROUP BY s.id, s.symbol, s.name, s.current_price, s.available_quantity
        ''')
        
        stocks = []
        for row in cursor.fetchall():
            stock_data = dict(row)
            # Calculate price volatility
            if stock_data['max_price'] and stock_data['min_price']:
                volatility = ((stock_data['max_price'] - stock_data['min_price']) / stock_data['min_price']) * 100
            else:
                volatility = 0
            
            stock_data['volatility_percent'] = volatility
            stocks.append(stock_data)
        
        conn.close()
        return {"success": True, "stocks": stocks}
    
    def get_top_users(self, limit=10):
        """Get top performing users"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        # Get all users with their portfolio values
        cursor.execute('''
            SELECT 
                u.id,
                u.username,
                u.balance,
                u.loan_amount,
                COALESCE(SUM(up.quantity * s.current_price), 0) as portfolio_value
            FROM users u
            LEFT JOIN user_portfolios up ON u.id = up.user_id
            LEFT JOIN stocks s ON up.stock_id = s.id
            GROUP BY u.id, u.username, u.balance, u.loan_amount
        ''')
        
        users = []
        for row in cursor.fetchall():
            user = dict(row)
            net_worth = user['balance'] + user['portfolio_value'] - user['loan_amount']
            total_pnl = net_worth - 10000.0  # Initial balance
            
            users.append({
                "username": user['username'],
                "balance": user['balance'],
                "portfolio_value": user['portfolio_value'],
                "loan_amount": user['loan_amount'],
                "net_worth": net_worth,
                "total_pnl": total_pnl
            })
        
        # Sort by total PnL
        users.sort(key=lambda x: x['total_pnl'], reverse=True)
        
        conn.close()
        return {"success": True, "top_users": users[:limit]}
    
    def get_top_stocks(self, limit=10):
        """Get top performing stocks"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT 
                s.symbol,
                s.name,
                s.current_price,
                COUNT(t.id) as transaction_count,
                SUM(CASE WHEN t.transaction_type = 'BUY' THEN t.quantity ELSE 0 END) as total_volume,
                AVG(sph.price) as avg_price
            FROM stocks s
            LEFT JOIN transactions t ON s.id = t.stock_id
            LEFT JOIN stock_price_history sph ON s.id = sph.stock_id
            GROUP BY s.id, s.symbol, s.name, s.current_price
            HAVING COUNT(sph.id) > 1
        ''')
        
        stocks = []
        for row in cursor.fetchall():
            stock = dict(row)
            if stock['avg_price']:
                price_performance = ((stock['current_price'] - stock['avg_price']) / stock['avg_price']) * 100
            else:
                price_performance = 0
            
            stock['price_performance_percent'] = price_performance
            stocks.append(stock)
        
        # Sort by transaction volume and performance
        stocks.sort(key=lambda x: (x['total_volume'] or 0, x['price_performance_percent']), reverse=True)
        
        conn.close()
        return {"success": True, "top_stocks": stocks[:limit]}

# Flask application setup
app = Flask(__name__)
api = Api(app, version='1.0', title='Stock Trading Simulation API',
          description='A comprehensive stock trading simulation system with real-time updates',
          doc='/')

# Initialize trading system
trading_system = StockTradingSystem()
trading_system.start_price_updates()

# API Models for Swagger documentation
stock_ns = Namespace('stocks', description='Stock management operations')
user_ns = Namespace('users', description='User and trading operations')

api.add_namespace(stock_ns)
api.add_namespace(user_ns)

# Request/Response models
stock_register_model = api.model('StockRegister', {
    'symbol': fields.String(required=True, description='Stock symbol'),
    'name': fields.String(required=True, description='Company name'),
    'price': fields.Float(required=True, description='Initial stock price (1-100)', min=1, max=100),
    'quantity': fields.Integer(required=True, description='Available quantity', min=1)
})

loan_request_model = api.model('LoanRequest', {
    'user_id': fields.String(required=True, description='User ID'),
    'amount': fields.Float(required=True, description='Loan amount', min=1, max=100000)
})

buy_request_model = api.model('BuyRequest', {
    'user_id': fields.String(required=True, description='User ID'),
    'symbol': fields.String(required=True, description='Stock symbol'),
    'quantity': fields.Integer(required=True, description='Quantity to buy', min=1)
})

sell_request_model = api.model('SellRequest', {
    'user_id': fields.String(required=True, description='User ID'),
    'symbol': fields.String(required=True, description='Stock symbol'),
    'quantity': fields.Integer(required=True, description='Quantity to sell', min=1)
})

# Stock Management APIs
@stock_ns.route('/register')
class StockRegister(Resource):
    @api.expect(stock_register_model)
    def post(self):
        """Register a new stock"""
        data = request.json
        result = trading_system.register_stock(
            data['symbol'], data['name'], data['price'], data['quantity']
        )
        return result

@stock_ns.route('/history')
class StockHistory(Resource):
    def get(self):
        """Retrieve stock price history"""
        symbol = request.args.get('symbol')
        result = trading_system.get_stock_history(symbol)
        return {"success": True, "history": result}

@stock_ns.route('/report')
class StockReport(Resource):
    def get(self):
        """Get stock-wise performance report"""
        return trading_system.get_stock_report()

@stock_ns.route('/top')
class TopStocks(Resource):
    def get(self):
        """List top-performing stocks"""
        limit = request.args.get('limit', 10, type=int)
        return trading_system.get_top_stocks(limit)

# User Management APIs
@user_ns.route('/loan')
class UserLoan(Resource):
    @api.expect(loan_request_model)
    def post(self):
        """Allow users to take a loan"""
        data = request.json
        return trading_system.take_loan(data['user_id'], data['amount'])

@user_ns.route('/buy')
class UserBuy(Resource):
    @api.expect(buy_request_model)
    def post(self):
        """Buy stocks"""
        data = request.json
        return trading_system.buy_stock(data['user_id'], data['symbol'], data['quantity'])

@user_ns.route('/sell')
class UserSell(Resource):
    @api.expect(sell_request_model)
    def post(self):
        """Sell owned stocks"""
        data = request.json
        return trading_system.sell_stock(data['user_id'], data['symbol'], data['quantity'])

@user_ns.route('/report')
class UserReport(Resource):
    def get(self):
        """Fetch user profit/loss report"""
        user_id = request.args.get('user_id', required=True)
        return trading_system.get_user_report(user_id)

@user_ns.route('/top')
class TopUsers(Resource):
    def get(self):
        """List top-performing users"""
        limit = request.args.get('limit', 10, type=int)
        return trading_system.get_top_users(limit)

# Utility endpoints
@api.route('/users/list')
class UsersList(Resource):
    def get(self):
        """Get list of all users (for testing)"""
        conn = trading_system.db.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id, username, balance, loan_amount FROM users")
        users = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return {"users": users}

@api.route('/stocks/list')
class StocksList(Resource):
    def get(self):
        """Get list of all stocks (for testing)"""
        conn = trading_system.db.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT symbol, name, current_price, available_quantity FROM stocks")
        stocks = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return {"stocks": stocks}

# Test function to simulate multiple users trading simultaneously
def simulate_trading_session():
    """
    Test function to simulate 5-10 users trading simultaneously
    """
    print("Starting trading simulation...")
    
    # Get all users and stocks
    conn = trading_system.db.get_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, username FROM users LIMIT 10")
    users = [dict(row) for row in cursor.fetchall()]
    
    cursor.execute("SELECT symbol FROM stocks")
    stocks = [row['symbol'] for row in cursor.fetchall()]
    
    conn.close()
    
    def simulate_user_trading(user):
        """Simulate trading for a single user"""
        user_id = user['id']
        username = user['username']
        
        print(f"Starting simulation for {username}")
        
        # Simulate random trading actions
        for _ in range(random.randint(3, 8)):  # 3-8 trades per user
            action = random.choice(['buy', 'sell', 'loan'])
            
            if action == 'buy':
                symbol = random.choice(stocks)
                quantity = random.randint(1, 10)
                result = trading_system.buy_stock(user_id, symbol, quantity)
                print(f"{username} BUY {quantity} {symbol}: {result.get('message', 'Success' if result.get('success') else 'Failed')}")
                
            elif action == 'sell':
                symbol = random.choice(stocks)
                quantity = random.randint(1, 5)
                result = trading_system.sell_stock(user_id, symbol, quantity)
                print(f"{username} SELL {quantity} {symbol}: {result.get('message', 'Success' if result.get('success') else 'Failed')}")
                
            elif action == 'loan':
                amount = random.randint(1000, 5000)
                result = trading_system.take_loan(user_id, amount)
                print(f"{username} LOAN ${amount}: {result.get('message', 'Success' if result.get('success') else 'Failed')}")
            
            time.sleep(random.uniform(0.1, 0.5))  # Small delay between actions
    
    # Use ThreadPoolExecutor to simulate concurrent trading
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(simulate_user_trading, user) for user in users[:random.randint(5, 10)]]
        
        # Wait for all trading to complete
        for future in futures:
            future.result()
    
    print("Trading simulation completed!")
    
    # Generate summary report
    print("\n=== SIMULATION SUMMARY ===")
    top_users = trading_system.get_top_users(5)
    print("Top 5 Users:")
    for i, user in enumerate(top_users['top_users'], 1):
        print(f"{i}. {user['username']}: Net Worth ${user['net_worth']:.2f}, PnL ${user['total_pnl']:.2f}")
    
    top_stocks = trading_system.get_top_stocks(5)
    print("\nTop 5 Stocks:")
    for i, stock in enumerate(top_stocks['top_stocks'], 1):
        print(f"{i}. {stock['symbol']}: Price ${stock['current_price']:.2f}, Volume {stock['total_volume'] or 0}")

# Background task management
@api.route('/system/start-updates')
class StartUpdates(Resource):
    def post(self):
        """Start the price update background task"""
        trading_system.start_price_updates()
        return {"success": True, "message": "Price updates started"}

@api.route('/system/stop-updates')
class StopUpdates(Resource):
    def post(self):
        """Stop the price update background task"""
        trading_system.stop_price_update_thread()
        return {"success": True, "message": "Price updates stopped"}

@api.route('/system/simulate-trading')
class SimulateTrading(Resource):
    def post(self):
        """Run trading simulation with 5-10 users"""
        try:
            # Run simulation in a separate thread to avoid blocking
            simulation_thread = threading.Thread(target=simulate_trading_session)
            simulation_thread.start()
            return {"success": True, "message": "Trading simulation started"}
        except Exception as e:
            return {"success": False, "message": str(e)}

@api.route('/system/force-price-update')
class ForcePriceUpdate(Resource):
    def post(self):
        """Manually trigger stock price update"""
        try:
            trading_system.update_stock_prices()
            return {"success": True, "message": "Stock prices updated successfully"}
        except Exception as e:
            return {"success": False, "message": str(e)}

# Health check endpoint
@api.route('/health')
class HealthCheck(Resource):
    def get(self):
        """System health check"""
        conn = trading_system.db.get_connection()
        cursor = conn.cursor()
        
        # Check database connectivity and get basic stats
        cursor.execute("SELECT COUNT(*) FROM users")
        user_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM stocks")
        stock_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM transactions")
        transaction_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT AVG(current_price) FROM stocks")
        avg_stock_price = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "stats": {
                "users": user_count,
                "stocks": stock_count,
                "transactions": transaction_count,
                "avg_stock_price": round(avg_stock_price or 0, 2)
            },
            "price_updates_active": trading_system.price_update_thread and trading_system.price_update_thread.is_alive()
        }

if __name__ == '__main__':
    try:
        print("🚀 Starting Stock Trading Simulation System...")
        print("📊 Database initialized with sample data")
        print("💹 Background price updates started (every 5 minutes)")
        print("🌐 API Documentation available at: http://localhost:5000/")
        print("❤️  Health check available at: http://localhost:5000/health")
        print("\n📋 Available API Endpoints:")
        print("   POST /stocks/register - Register new stock")
        print("   GET  /stocks/history - Get price history")
        print("   GET  /stocks/report - Stock performance report")
        print("   GET  /stocks/top - Top performing stocks")
        print("   POST /users/loan - Take a loan")
        print("   POST /users/buy - Buy stocks")
        print("   POST /users/sell - Sell stocks")
        print("   GET  /users/report - User performance report")
        print("   GET  /users/top - Top performing users")
        print("   POST /system/simulate-trading - Run trading simulation")
        print("\n🧪 To run trading simulation:")
        print("   curl -X POST http://localhost:5000/system/simulate-trading")
        print("\n🔥 Starting Flask server...")
        
        app.run(debug=True, host='0.0.0.0', port=5000, threaded=True)
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")
        trading_system.stop_price_update_thread()
        print("✅ System shutdown complete")