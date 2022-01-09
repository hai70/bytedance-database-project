#include "row_table.h"
#include <cstring>
#include<iostream>

namespace bytedance_db_project {
RowTable::RowTable() {}

RowTable::~RowTable() {
  if (rows_ != nullptr) {
    delete rows_;
    rows_ = nullptr;
  }
}

void RowTable::Load(BaseDataLoader *loader) {
  num_cols_ = loader->GetNumCols();
  auto rows = loader->GetRows();
  num_rows_ = rows.size();
  rows_ = new char[FIXED_FIELD_LEN * num_rows_ * num_cols_];
  for (auto row_id = 0; row_id < num_rows_; row_id++) {
    auto cur_row = rows.at(row_id);
    std::memcpy(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_), cur_row,
                FIXED_FIELD_LEN * num_cols_);
  }
}

int32_t RowTable::GetIntField(int32_t row_id, int32_t col_id) {
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
  int32_t* ptrToIntField = (int32_t*)(rows_ + (row_id * num_cols_ + col_id) * FIXED_FIELD_LEN);
  return *ptrToIntField;
}

void RowTable::PutIntField(int32_t row_id, int32_t col_id, int32_t field) {
  // TODO: Implement this!
  try{
    if(row_id < 0 || row_id >= num_rows_){
      throw "row_id is invalid";
    }
    if(col_id < 0 || col_id >= num_cols_){
      throw "col_id is invalid";
    }

    int32_t* ptrToIntField = (int32_t*)(rows_ + (row_id * num_cols_ + col_id) * FIXED_FIELD_LEN);
    *ptrToIntField  = field;
  }catch(const char* msg){
    std::cerr << msg << std::endl;
  }
}

int64_t RowTable::ColumnSum() {
  // TODO: Implement this!
  int32_t* ptrToIntFieldInCol0 = (int32_t*)rows_;
  int64_t col0Sum = 0;
  for(int32_t i = 0; i < num_rows_; i++){
    ptrToIntFieldInCol0 = (int32_t*)rows_ + i * num_cols_;
    col0Sum += *ptrToIntFieldInCol0;
  }
  return col0Sum;
}

int64_t RowTable::PredicatedColumnSum(int32_t threshold1, int32_t threshold2) {
  // TODO: Implement this!
  return 0;
}

int64_t RowTable::PredicatedAllColumnsSum(int32_t threshold) {
  // TODO: Implement this!
  return 0;
}

int64_t RowTable::PredicatedUpdate(int32_t threshold) {
  // TODO: Implement this!
  return 0;
}
} // namespace bytedance_db_project