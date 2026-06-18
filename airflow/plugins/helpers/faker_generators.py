"""
Faker-based data generators for the e-commerce OLTP database.
Each method generates realistic data for a specific table or group of tables.
"""

import random
import logging
from datetime import datetime, timedelta
from faker import Faker

fake = Faker("vi_VN")  # Vietnamese locale for realistic names/addresses
Faker.seed(42)

logger = logging.getLogger(__name__)

# =============================================
# Constants
# =============================================

VIETNAM_REGIONS = [
    "Đông Bắc Bộ",
    "Tây Bắc Bộ",
    "Đồng bằng sông Hồng",
    "Bắc Trung Bộ",
    "Duyên hải Nam Trung Bộ",
    "Tây Nguyên",
    "Đông Nam Bộ",
    "Đồng bằng sông Cửu Long",
]

VIETNAM_PROVINCES = {
    "Đông Bắc Bộ": [
        ("Hà Giang", 22.8233, 104.9833),
        ("Cao Bằng", 22.6667, 106.2583),
        ("Bắc Kạn", 22.1500, 105.8333),
        ("Tuyên Quang", 21.8167, 105.2167),
        ("Lào Cai", 22.4833, 103.9500),
        ("Lạng Sơn", 21.8500, 106.7500),
        ("Thái Nguyên", 21.5667, 105.8250),
        ("Bắc Giang", 21.2833, 106.2000),
        ("Phú Thọ", 21.4000, 105.1667),
        ("Quảng Ninh", 21.0000, 107.3000),
    ],
    "Tây Bắc Bộ": [
        ("Điện Biên", 21.3833, 103.0167),
        ("Lai Châu", 22.3833, 103.4500),
        ("Sơn La", 21.3333, 103.9000),
        ("Hòa Bình", 20.8167, 105.3333),
        ("Yên Bái", 21.7000, 104.8667),
    ],
    "Đồng bằng sông Hồng": [
        ("Hà Nội", 21.0285, 105.8542),
        ("Hải Phòng", 20.8449, 106.6881),
        ("Vĩnh Phúc", 21.3089, 105.6047),
        ("Bắc Ninh", 21.1833, 106.0500),
        ("Hải Dương", 20.9333, 106.3167),
        ("Hưng Yên", 20.6500, 106.0667),
        ("Hà Nam", 20.5833, 105.9167),
        ("Nam Định", 20.4333, 106.1833),
        ("Thái Bình", 20.4500, 106.3333),
        ("Ninh Bình", 20.2500, 105.9750),
    ],
    "Bắc Trung Bộ": [
        ("Thanh Hóa", 19.8000, 105.7833),
        ("Nghệ An", 19.3333, 104.8333),
        ("Hà Tĩnh", 18.3333, 105.9000),
        ("Quảng Bình", 17.4667, 106.6000),
        ("Quảng Trị", 16.7333, 107.1833),
        ("Thừa Thiên Huế", 16.4667, 107.6000),
    ],
    "Duyên hải Nam Trung Bộ": [
        ("Đà Nẵng", 16.0544, 108.2022),
        ("Quảng Nam", 15.5333, 108.0167),
        ("Quảng Ngãi", 15.1167, 108.8000),
        ("Bình Định", 13.7667, 109.2167),
        ("Phú Yên", 13.0833, 109.3167),
        ("Khánh Hòa", 12.2500, 109.1833),
        ("Ninh Thuận", 11.5833, 108.9833),
        ("Bình Thuận", 10.9333, 108.1000),
    ],
    "Tây Nguyên": [
        ("Kon Tum", 14.3500, 108.0000),
        ("Gia Lai", 13.9833, 108.0000),
        ("Đắk Lắk", 12.6667, 108.0333),
        ("Đắk Nông", 12.0000, 107.6833),
        ("Lâm Đồng", 11.9500, 108.4333),
    ],
    "Đông Nam Bộ": [
        ("TP. Hồ Chí Minh", 10.8231, 106.6297),
        ("Bà Rịa - Vũng Tàu", 10.5000, 107.1667),
        ("Bình Dương", 11.1667, 106.6333),
        ("Bình Phước", 11.7500, 106.9000),
        ("Đồng Nai", 10.9500, 106.8333),
        ("Tây Ninh", 11.3000, 106.1000),
    ],
    "Đồng bằng sông Cửu Long": [
        ("Long An", 10.5333, 106.4000),
        ("Tiền Giang", 10.3500, 106.3500),
        ("Bến Tre", 10.2333, 106.3833),
        ("Trà Vinh", 9.9333, 106.3333),
        ("Vĩnh Long", 10.2500, 105.9667),
        ("Đồng Tháp", 10.4500, 105.6333),
        ("An Giang", 10.3833, 105.4333),
        ("Kiên Giang", 10.0000, 105.0833),
        ("Cần Thơ", 10.0333, 105.7833),
        ("Hậu Giang", 9.7833, 105.4667),
        ("Sóc Trăng", 9.6000, 105.9667),
        ("Bạc Liêu", 9.2833, 105.7167),
        ("Cà Mau", 9.1833, 105.1500),
    ],
}

PARENT_CATEGORIES = [
    "Điện thoại & Phụ kiện",
    "Máy tính & Laptop",
    "Thiết bị điện tử",
    "Thời trang Nam",
    "Thời trang Nữ",
    "Nhà cửa & Đời sống",
    "Sức khỏe & Làm đẹp",
    "Thể thao & Du lịch",
    "Sách, VPP & Quà tặng",
    "Ô tô, Xe máy & Phụ kiện",
]

SUB_CATEGORIES = {
    "Điện thoại & Phụ kiện": ["Smartphone", "Ốp lưng", "Cáp sạc", "Tai nghe", "Pin dự phòng"],
    "Máy tính & Laptop": ["Laptop", "PC để bàn", "Màn hình", "Bàn phím", "Chuột"],
    "Thiết bị điện tử": ["TV", "Loa", "Máy ảnh", "Máy chiếu", "Đồng hồ thông minh"],
    "Thời trang Nam": ["Áo", "Quần", "Giày", "Túi xách", "Đồng hồ"],
    "Thời trang Nữ": ["Váy đầm", "Áo nữ", "Giày nữ", "Túi xách nữ", "Phụ kiện thời trang"],
    "Nhà cửa & Đời sống": ["Nội thất", "Đồ dùng nhà bếp", "Trang trí", "Đèn", "Dụng cụ"],
    "Sức khỏe & Làm đẹp": ["Mỹ phẩm", "Chăm sóc da", "Nước hoa", "Thực phẩm chức năng", "Dụng cụ làm đẹp"],
    "Thể thao & Du lịch": ["Giày thể thao", "Quần áo thể thao", "Dụng cụ tập gym", "Đồ cắm trại", "Vali"],
    "Sách, VPP & Quà tặng": ["Sách", "Vở", "Bút", "Quà tặng", "Đồ handmade"],
    "Ô tô, Xe máy & Phụ kiện": ["Phụ kiện ô tô", "Phụ kiện xe máy", "Dầu nhớt", "GPS", "Camera hành trình"],
}

PRODUCT_TAGS = [
    "bestseller", "new_arrival", "sale", "hot_deal", "limited_edition",
    "eco_friendly", "premium", "budget", "trending", "exclusive",
    "free_shipping", "gift_idea", "back_in_stock", "clearance", "seasonal",
]

BRAND_PREFIXES = [
    "Viet", "Sai", "Ha", "Tech", "Smart", "Neo", "Pro", "Ultra",
    "Eco", "Star", "Green", "Sun", "Moon", "Gold", "Blue",
]

BRAND_SUFFIXES = [
    "Tech", "Corp", "Shop", "Store", "Hub", "Lab", "Works",
    "Mart", "Plus", "Zone", "Link", "Way", "One", "Max", "Go",
]


class FakeDataGenerator:
    """Central class for generating fake e-commerce data."""

    def __init__(self):
        self.fake = fake

    # ----- Geography -----

    def generate_regions(self):
        """Return list of region tuples: (region_name,)"""
        return [(name,) for name in VIETNAM_REGIONS]

    def generate_provinces(self, region_id_map):
        """
        Return list of province tuples: (province_name, region_id, lat, lon)

        Args:
            region_id_map: dict mapping region_name -> region_id from DB
        """
        provinces = []
        for region_name, province_list in VIETNAM_PROVINCES.items():
            region_id = region_id_map.get(region_name)
            if region_id:
                for name, lat, lon in province_list:
                    provinces.append((name, region_id, lat, lon))
        return provinces

    # ----- Tags -----

    def generate_tags(self):
        """Return list of tag tuples: (tag_name,)"""
        return [(tag,) for tag in PRODUCT_TAGS]

    # ----- Brands -----

    def generate_brands(self, count=5):
        """Generate random brand names."""
        brands = []
        for _ in range(count):
            name = f"{random.choice(BRAND_PREFIXES)}{random.choice(BRAND_SUFFIXES)}"
            brands.append((name,))
        return brands

    # ----- Categories -----

    def generate_parent_categories(self):
        """Return list of parent category tuples: (category_name, parent_id, slug)"""
        categories = []
        for name in PARENT_CATEGORIES:
            slug = name.lower().replace(" ", "-").replace("&", "and").replace(",", "")
            categories.append((name, None, slug))
        return categories

    def generate_sub_categories(self, parent_id_map):
        """
        Return list of sub-category tuples: (category_name, parent_id, slug)

        Args:
            parent_id_map: dict mapping parent_category_name -> parent_id from DB
        """
        subs = []
        for parent_name, sub_list in SUB_CATEGORIES.items():
            parent_id = parent_id_map.get(parent_name)
            if parent_id:
                for sub_name in sub_list:
                    slug = sub_name.lower().replace(" ", "-")
                    subs.append((sub_name, parent_id, slug))
        return subs

    # ----- Campaigns & Discounts -----

    def generate_campaigns(self, count=3):
        """Generate marketing campaigns with start/end dates."""
        campaigns = []
        for i in range(count):
            title = f"Campaign_{self.fake.bothify('???_##')}"
            start = self.fake.date_time_between(start_date="-30d", end_date="now")
            end = start + timedelta(days=random.randint(7, 60))
            campaigns.append((title, start, end))
        return campaigns

    def generate_discounts(self, campaign_ids, count_per_campaign=2):
        """Generate discount codes linked to campaigns."""
        discounts = []
        for cid in campaign_ids:
            for _ in range(count_per_campaign):
                disc_type = random.choice(["percent", "amount"])
                value = (
                    random.choice([5, 10, 15, 20, 25, 30])
                    if disc_type == "percent"
                    else random.choice([10000, 20000, 50000, 100000])
                )
                code = self.fake.bothify("SALE-????-####").upper()
                start = self.fake.date_time_between(start_date="-30d", end_date="now")
                end = start + timedelta(days=random.randint(7, 30))
                discounts.append((cid, disc_type, value, code, start, end))
        return discounts

    # ----- Products -----

    def generate_products(self, category_ids, brand_ids, count=10):
        """Generate products linked to categories and brands."""
        products = []
        for _ in range(count):
            name = f"{self.fake.word().capitalize()} {self.fake.word().capitalize()} {random.choice(['Pro', 'Max', 'Plus', 'Lite', 'S', 'X', ''])}"
            name = name.strip()
            cat_id = random.choice(category_ids)
            brand_id = random.choice(brand_ids)
            price = round(random.uniform(50000, 50000000), 2)
            cost = round(price * random.uniform(0.3, 0.7), 2)
            qty = random.randint(10, 5000)
            products.append((name, cat_id, brand_id, price, cost, qty))
        return products

    # ----- Users -----

    def generate_users(self, count=10):
        """Generate user accounts with Vietnamese names."""
        users = []
        for _ in range(count):
            username = self.fake.user_name() + str(random.randint(100, 9999))
            password = self.fake.sha256()[:60]
            email = f"{username}@{random.choice(['gmail.com', 'yahoo.com', 'outlook.com'])}"
            mobile = self.fake.phone_number()
            users.append((username, password, email, mobile))
        return users

    # ----- Orders -----

    def generate_order(self, user_id, address_id, product_list,
                       payment_method_ids, shipping_method_ids,
                       order_status_ids, payment_status_ids,
                       shipping_status_ids, discount_id=None):
        """
        Generate a complete order with order details.

        Args:
            user_id: customer user ID
            address_id: delivery address ID
            product_list: list of dicts with keys: id, product_price
            payment_method_ids: list of valid payment method IDs
            shipping_method_ids: list of valid shipping method IDs
            order_status_ids: list of valid order status IDs
            payment_status_ids: list of valid payment status IDs
            shipping_status_ids: list of valid shipping status IDs
            discount_id: optional discount ID to apply

        Returns:
            tuple: (order_data, order_details_list)
        """
        # Pick 1-5 random products for this order
        num_items = random.randint(1, min(5, len(product_list)))
        selected_products = random.sample(product_list, num_items)

        # Calculate order details
        order_details = []
        order_amount = 0

        for product in selected_products:
            qty = random.randint(1, 5)
            price = float(product["product_price"])
            tax = round(price * 0.1, 2)  # 10% VAT
            subtotal = round((price + tax) * qty, 2)
            order_amount += subtotal
            order_details.append({
                "product_id": product["id"],
                "quantity": qty,
                "product_price": price,
                "product_tax": tax,
                "subtotal_amount": subtotal,
            })

        # Calculate discount
        discount_amount = 0
        if discount_id:
            discount_amount = round(order_amount * random.uniform(0.05, 0.2), 2)

        total_amount = round(order_amount - discount_amount, 2)

        order_data = {
            "user_id": user_id,
            "staff_id": None,
            "address_id": address_id,
            "order_amount": order_amount,
            "discount_amount": discount_amount,
            "total_amount": total_amount,
            "discount_id": discount_id,
            "payment_method_id": random.choice(payment_method_ids),
            "payment_status_id": random.choice(payment_status_ids),
            "order_status_id": random.choice(order_status_ids),
            "shipping_method_id": random.choice(shipping_method_ids),
            "shipping_status_id": random.choice(shipping_status_ids),
        }

        return order_data, order_details
