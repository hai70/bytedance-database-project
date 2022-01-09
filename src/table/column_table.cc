#include "column_table.h"
#include <cstring>
#include <iostream>

namespace bytedance_db_project {
ColumnTable::ColumnTable() {}

ColumnTable::~ColumnTable() {
  if (columns_ != nullptr) {
    delete columns_;
    columns_ = nullptr;
  }
}

//
// columnTable, which stores data in column-major format.
// That is, data is laid out like
//   col 1 | col 2 | ... | col m.
//
void ColumnTable::Load(BaseDataLoader *loader) {
  num_cols_ = loader->GetNumCols();
  std::vector<char *> rows = loader->GetRows();
  num_rows_ = rows.size();
  columns_ = new char[FIXED_FIELD_LEN * num_rows_ * num_cols_];

  for (int32_t row_id = 0; row_id < num_rows_; row_id++) {
    auto cur_row = rows.at(row_id);
    for (int32_t col_id = 0; col_id < num_cols_; col_id++) {
      //offset是columns_的offset，将rows按行遍历，每一个field放到columns_的合适位置
      //例如，rows第0行每个字段间隔地放到columns_中（间隔距离为num_rows_）
      int32_t offset = FIXED_FIELD_LEN * ((col_id * num_rows_) + row_id);
      std::memcpy(columns_ + offset, cur_row + FIXED_FIELD_LEN * col_id,
                  FIXED_FIELD_LEN);
    }
  }
}

int32_t ColumnTable::GetIntField(int32_t row_id, int32_t col_id) {
  // TODO: Implement this!
  try{
    if(row_id < 0 || row_id >= num_rows_){
      throw "row_id is invalid";
    }
    if(col_id < 0 || col_id >= num_cols_){
      throw "col_id is invalid";
    }
  }catch(const char* msg){
    std::cerr << msg << std::endl;
  }
  int32_t* ptrToIntField = (int32_t*)(columns_ + (col_id * num_rows_ + row_id) * FIXED_FIELD_LEN);
  return *ptrToIntField;
  
}

void ColumnTable::PutIntField(int32_t row_id, int32_t col_id, int32_t field) {
  // TODO: Implement this!
  try{
    if(row_id < 0 || row_id >= num_rows_){
      throw "row_id is invalid";
    }
    if(col_id < 0 || col_id >= num_cols_){
      throw "col_id is invalid";
    }

    int32_t* ptrToIntField = (int32_t*)(columns_ + (col_id * num_rows_ + row_id) * FIXED_FIELD_LEN);
    
    *ptrToIntField = field;
  }catch(const char* msg){
    std::cerr << msg << std::endl;
  }
}

int64_t ColumnTable::ColumnSum() {
  // TODO: Implement this!
  int32_t* ptrToIntFieldInCol0 = (int32_t*)columns_;
  int64_t col0Sum = 0;
  for(int32_t i = 0; i < num_rows_; i++){
    ptrToIntFieldInCol0 = (int32_t*)columns_ + i;
    col0Sum += *ptrToIntFieldInCol0;
  }
  return col0Sum;
}

int64_t ColumnTable::PredicatedColumnSum(int32_t threshold1,
                                         int32_t threshold2) {
  // TODO: Implement this!
  return 0;
}

int64_t ColumnTable::PredicatedAllColumnsSum(int32_t threshold) {
  // TODO: Implement this!
  return 0;
}

int64_t ColumnTable::PredicatedUpdate(int32_t threshold) {
  // TODO: Implement this!
  return 0;
}
} // namespace bytedance_db_project