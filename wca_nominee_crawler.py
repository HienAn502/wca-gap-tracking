import requests
import re

class CrawlNominees:
    def __init__(self):
        pass

    def get_content_between(self, text, start, end, group=1):
        pattern = f'{start}(.*?){end}'
        match = re.search(pattern, text, re.DOTALL)
        return match.group(group) if match else ''
    
    def parse_blocks(self, content, start_pattern, tag_type):
        blocks = []
        current_block = ""
        in_block = False
        tag_count = 0

        for line in content.splitlines():
            if not in_block and start_pattern in line:
                in_block = True
                tag_count = 1
                current_block = line
            elif in_block:
                current_block += "\n" + line
                if f'<{tag_type}' in line:
                    tag_count += 1
                if f'</{tag_type}>' in line:
                    tag_count -= 1
                    if tag_count == 0:
                        blocks.append(current_block)
                        current_block = ""
                        in_block = False

        return blocks

    def get_wy_category_blocks(self, html_content):
        return self.parse_blocks(
            html_content,
            'wy-category',
            'div'
        )
    
    def get_subcate_blocks(self, category_block):
        return self.parse_blocks(
            category_block,
            'subCate',
            'div'
        )
    
    def get_list_nominees(self, container_block):
        return self.parse_blocks(
            container_block,
            'listNominees',
            'ul'
        )

    def extract_nominee_data(self, nominee_block):
        nominee_data = {
            'data_member': self.get_content_between(nominee_block, 'data-member="', '"'),
            'ava_link': self.get_content_between(nominee_block, '<img src="', '"'),
            'nominee_name': self.get_content_between(self.get_content_between(nominee_block, '<h3 class="nominee-name">', '</h3>'), ">", "<").strip(),
            'nominee_des': self.get_content_between(nominee_block, '<div class="nominee-title">', '</div>'),
        }

        return nominee_data
    
    def get_nominees(self, award_block):
        list_nominees = self.get_list_nominees(award_block)

        if not list_nominees:
            return []

        nominees = []

        for list_nominee_block in list_nominees:
            for line in list_nominee_block.splitlines():
                if '<li' in line and 'nominee js-vote-wrapt' in line.lower():
                    li_blocks = self.parse_blocks(list_nominee_block, line.strip(), 'li')
                    for li_block in li_blocks:
                        nominees.append(self.extract_nominee_data(li_block))

        return nominees

    def crawl_nominees(self):
        wca_nominees = {}

        url = "https://weyoung.vn/"

        print(f"Crawling nominees from {url}")
        response = requests.get(url, headers={'Accept-Encoding': 'gzip'})
        html = response.text
        print(f"Response status code: {response.status_code}")

        category_blocks = self.get_wy_category_blocks(html)

        print(f"Found {len(category_blocks)} wy-category blocks")
        
        for category_block in category_blocks:
            category_tag = self.get_content_between(category_block, '<div class="wy-category ', '"')
            
            subcate_blocks = self.get_subcate_blocks(category_block)

            if subcate_blocks:
                print(f"Category '{category_tag}' has {len(subcate_blocks)} subcategories")
                wca_nominees[category_tag] = {
                    "subcategories": {}
                }

                for subcate_block in subcate_blocks:
                    award_name = self.get_content_between(subcate_block, '<h2 id="award-name-(.*?)">', '</h2>', 2)
                    award_id = self.get_content_between(subcate_block, '<h2 id="award-name-', '"')
                    print(f"Award '{award_name}' has id '{award_id}' in category '{category_tag}'")

                    nominees = self.get_nominees(subcate_block)
                    print(f"Found {len(nominees)} nominees in award '{award_name}'")

                    wca_nominees[category_tag]['subcategories'][award_id] = {
                        "award_name": award_name,
                        "nominees": nominees
                    }
            else:
                award_name = self.get_content_between(category_block, '<div id="award-name-(.*?)">', '</div>', 2)
                award_id = self.get_content_between(category_block, '<div id="award-name-', '"')

                print(f"Award '{award_name}' has id '{award_id}' in category '{category_tag}'")

                nominees = self.get_nominees(category_block)
                print(f"Found {len(nominees)} nominees in category '{category_tag}'")

                wca_nominees[category_tag] = {
                    award_id: {
                        "award_name": award_name,
                        "nominees": nominees
                    }
                }

        return wca_nominees

if __name__ == "__main__":
    crawler = CrawlNominees()
    wca_nominees = crawler.crawl_nominees()