require 'socket'

POSTGRESQLISH_SERVER_PORT = '25432'

# 自身を含むメッセージ内容のバイト単位の長さ (定義的にはInt32なので、4バイト)
LENGTH_OF_MESSAGE_CONTENTS = 4

# PostgreSQLのOID列の値。ただしOID列はサポートされていないため、固定値を返すことになる
# https://www.postgresql.jp/document/16/html/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-COMMANDCOMPLETE
OID = 0

# RubyのArray#packについて
# https://docs.ruby-lang.org/ja/latest/method/Array/i/pack.html
def sb(value)
  # 文字列をバイナリ文字列にする
  [value].pack('A*')
end

def sbn(value)
  # 文字列の末尾にNULL終端文字列を追加してバイナリ文字列にする
  [value].pack('Z*')
end

def i32b(value)
  # ビッグエンディアン、32ビット符号つき整数としてバイナリ文字列にする
  [value].pack('l>')
end

def i16b(value)
  # ビッグエンディアン、16ビット符号つき整数としてバイナリ文字列にする
  [value].pack('s>')
end

def b32i(value)
  # バイナリ文字列を 32ビット符号つき整数 にする
  value.unpack1('l>')
end

def ssl_request(sock)
  # SSL Request で受け取った値は捨てて良い
  # 厳密には検証したほうが良さそう
  sock.recvmsg

  # SSL接続が不可なので、 'N' を返す
  sock.write 'N'
end

def startup_message(sock)
  # Startup Message では、クライアントからパケットが送られてくるが、すべて捨てて良い
  sock.recvmsg
end

def send_authentication_ok(sock)
  # 認証要求メッセージを送るが、パスワード設定は無視するため、OKでよい
  # https://www.postgresql.jp/document/16/html/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-AUTHENTICATIONOK
  msg = sb('R') + i32b(8) + i32b(0)
  sock.write msg
end

def send_ready_for_query(sock)
  # https://www.postgresql.jp/document/16/html/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-READYFORQUERY
  sock.write sb('Z') + i32b(5) + sb('I')
end

def receive_tag(sock)
  # Socket#readbyteで1バイトだけ読み込む
  # https://docs.ruby-lang.org/ja/latest/method/IO/i/readbyte.html
  sock.readbyte
end

def receive_message_contents(sock)
  length = sock.read LENGTH_OF_MESSAGE_CONTENTS

  # lengthには、メッセージ内容の長さ + バイト単位の長さ が設定されている
  # すでにバイト単位の長さの情報は recv しているので、それ以外の長さをメッセージ内容の長さと考えて recv する
  # なお、length はバイナリ文字列なので、32bit整数へと変換してから演算する
  sql = sock.read(b32i(length) - LENGTH_OF_MESSAGE_CONTENTS)

  # SQL文字列を受信した想定だが、末尾に NULL終端文字列 が入っている可能性がある
  # そこで、 unpack('A*') で NULL終端文字列 を削除した後、再度 pack('A*') して、SQL文字列を取得している
  # また、SQLに大文字・小文字が混在していると取り扱いが手間かもしれないため、小文字だけにしておく
  sql.unpack('A*').pack('A*').downcase
end

def send_command_complete_of_create_table(sock)
  value = 'CREATE TABLE'
  value_length = value.bytesize + 1 # NULL終端文字列の分も長さとして計算する

  # CommandCompleteのStringタグにはNULL終端文字列が必要
  msg = sb('C') + i32b(value_length + LENGTH_OF_MESSAGE_CONTENTS) + sbn(value)
  sock.write msg
end

def send_command_complete_of_insert(sock, sql)
  # 今回のINSERTは以下のSQL
  # insert into apples values (1, 'shinano_gold'), (2, 'fuji');
  # そのため、簡易的な実装として、 `(` の数 == 列数とみなす
  column_count = sql.count('(')
  value = "INSERT #{OID} #{column_count}"

  value_length = value.bytesize + 1 # NULL終端文字列の分も長さとして計算する

  # CREATE TABLE同様
  msg = sb('C') + i32b(value_length + LENGTH_OF_MESSAGE_CONTENTS) + sbn(value)
  sock.write msg
end

def col_id
  field_name = sbn('id')  # field name には NULL終端文字列 が必要
  object_id_of_table = 16385 # 適当な値
  column_id = 1

  # pg_type を参照する項目
  # https://www.postgresql.jp/document/16/html/catalog-pg-type.html
  pg_type_oid = i32b(23)
  pg_type_typlen = i16b(4)

  # pg_attribute を参照する項目
  # https://www.postgresql.jp/document/16/html/catalog-pg-attribute.html
  pg_attribute_attypmod = i32b(-1)
  format_code = i16b(0)

  field_name + i32b(object_id_of_table) + i16b(column_id) + pg_type_oid + pg_type_typlen + pg_attribute_attypmod + format_code
end

def col_name
  field_name = sbn('name')  # field name には NULL終端文字列 が必要
  object_id_of_table = 16385 # 適当な値
  column_id = 2

  # pg_type を参照する項目
  # https://www.postgresql.jp/document/16/html/catalog-pg-type.html
  pg_type_oid = i32b(1043)
  pg_type_typlen = i16b(-1)  # 可変長なので、 -1

  # pg_attribute を参照する項目
  # https://www.postgresql.jp/document/16/html/catalog-pg-attribute.html
  pg_attribute_attypmod = i32b(255+4)  # 指定された最大長に 4 を加える => 今回は 255 なので、それに4を加える
  format_code = i16b(0)

  field_name + i32b(object_id_of_table) + i16b(column_id) + pg_type_oid + pg_type_typlen + pg_attribute_attypmod + format_code
end

def send_row_description(sock)
  contents = col_id + col_name

  # length_of_message_contentsはInt32なので4バイト、number_of_columnはInt16なので2バイト
  # 上記2つに各列の内容を合わせたものが、メッセージ全体の長さ
  bytesize_for_length_of_message_contents = 4
  bytesize_for_number_of_column = 2
  message_bytesize = bytesize_for_length_of_message_contents + bytesize_for_number_of_column + contents.length

  # 今回のデータは2列
  number_of_column = 2

  msg = sb('T') + i32b(message_bytesize) + i16b(number_of_column) + contents
  sock.write(msg)
end

def data_column(column_number, column_value)
  i32b(column_value.to_s.length) + sb(column_value.to_s)
end

def data_row(col1_value, col2_value)
  # 1列目(id)の値
  content_of_col_id = data_column(1, col1_value)
  # 2列目(name)の値
  content_of_col_name = data_column(2, col2_value)
  # メッセージ内容は、1行目と2行目を連結させたもの
  contents = content_of_col_id + content_of_col_name

  # length_of_message_contentsはInt32なので4バイト、number_of_columnはInt16なので2バイト
  # 上記2つに各列の内容を合わせたものが、メッセージ全体の長さ
  bytesize_for_length_of_message_contents = 4
  bytesize_for_number_of_column = 2
  message_bytesize = bytesize_for_length_of_message_contents + bytesize_for_number_of_column + contents.length

  # 今回のデータは2列
  number_of_column = 2

  sb('D') + i32b(message_bytesize) + i16b(number_of_column) + contents
end

def send_data_row(sock)
  # 1行目のデータ
  data_row_of_shinano_gold = data_row(1, 'shinano_gold')
  # 2行目のデータ
  data_row_of_fuji = data_row(2, 'fuji')

  msg = data_row_of_shinano_gold + data_row_of_fuji
  sock.write msg
end

def send_command_complete_of_select(sock)
  # 結果行数は、今回固定で 2 とする
  value = "SELECT 2"

  value_length = value.bytesize + 1 # NULL終端文字列の分も長さとして計算する

  # CREATE TABLE同様
  msg = sb('C') + i32b(value_length + LENGTH_OF_MESSAGE_CONTENTS) + sbn(value)
  sock.write msg
end

def startup_phase(sock)
  ssl_request(sock)
  startup_message(sock)
  send_authentication_ok(sock)
  send_ready_for_query(sock)
end

def simple_query_phase(sock)
  tag = receive_tag(sock)

  # 簡易問い合わせでない場合、処理を終了する
  # なお、 sock.recv したときには数値になっているので、文字コード変換が必要
  return false if tag.chr != 'Q'

  # メッセージ内容を取得する
  sql = receive_message_contents(sock)

  case sql
  in String if sql.start_with?('create table')
    send_command_complete_of_create_table(sock)
    send_ready_for_query(sock)
  in String if sql.start_with?('insert')
    send_command_complete_of_insert(sock, sql)
    send_ready_for_query(sock)
  in String if sql.start_with?('select')
    send_row_description(sock)
    send_data_row(sock)
    send_command_complete_of_select(sock)
    send_ready_for_query(sock)
  else
    false
  end
end

Socket.tcp_server_loop(POSTGRESQLISH_SERVER_PORT) do |sock, addr_info|
  puts 'Start flow'

  startup_phase(sock)

  loop do
    break unless simple_query_phase(sock)
  end

ensure
  sock.close

  puts 'End flow'
end
