/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <ctype.h>
#include <sys/time.h>
#include <iostream>
#include <fstream>
#include <map>
#include <set>

#include <boost/lexical_cast.hpp>
#include <boost/program_options.hpp>

#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <boost/random/variate_generator.hpp>

#include "Compiler.hh"
#include "ValidSchema.hh"
#include "NodeImpl.hh"

using std::ostream;
using std::ifstream;
using std::ofstream;
using std::map;
using std::set;
using std::string;
using std::vector;
using avro::NodePtr;
using avro::resolveSymbol;

using boost::lexical_cast;

using avro::ValidSchema;
using avro::compileJsonSchema;


class CodeGen {
    size_t unionNumber_;
    std::ostream& os_;
    bool inNamespace_;
    const std::string ns_;
    const std::string schemaFile_;
    const std::string headerFile_;
    const std::string includePrefix_;
    const bool noUnion_;
    const bool implementation_;
    boost::mt19937 random_;

    map<NodePtr, string> done;
    set<NodePtr> doing;

    std::string guard();
    std::string fullname(const string& name) const;
    std::string objcfullname(const string& name) const;
    std::string generateEnumType(const NodePtr& n);
    std::string objcTypeOf(const NodePtr& n);
    std::string cppTypeOf(const NodePtr& n);
    std::string generateRecordType(const NodePtr& n);
    std::string unionName();
    std::string objcUnionName();
    std::string generateObjcInitializer(const NodePtr& node, const string& cppValue);
    std::string generateUnionType(const NodePtr& n);
    std::string generateType(const NodePtr& n);
    std::string generateDeclaration(const NodePtr& n);
    std::string doGenerateType(const NodePtr& n);
    void generateEnumImplementation(const NodePtr& n);
    void generateImplementation(const NodePtr& n);
    void generateRecordImplementation(const NodePtr& n);
    void generateUnionImplementation(const NodePtr& n);
    void emitCopyright();
public:
    CodeGen(std::ostream& os, const std::string& ns,
        const std::string& schemaFile, const std::string& headerFile,
        const std::string& includePrefix, bool noUnion, bool implementation) :
        unionNumber_(0), os_(os), inNamespace_(false), ns_(ns),
        schemaFile_(schemaFile), headerFile_(headerFile),
        includePrefix_(includePrefix), noUnion_(noUnion), implementation_(implementation),
        random_(::time(0)) { }
    void generate(const ValidSchema& schema);
};

static string decorate(const avro::Name& name)
{
    return name.simpleName();
}

string CodeGen::fullname(const string& name) const
{
    return ns_.empty() ? name : (ns_ + "::" + name);
}

string CodeGen::objcfullname(const string& name) const
{
    return name;
}

string CodeGen::generateEnumType(const NodePtr& n)
{
    os_ << "enum " << decorate(n->name()) << "Enum {\n";
    size_t c = n->names();
    for (size_t i = 0; i < c; ++i) {
        os_ << "    v_" << n->nameAt(i) << ",\n";
    }
    os_ << "};\n\n";
    return decorate(n->name());
}

string CodeGen::objcTypeOf(const NodePtr& n)
{    
    switch (n->type()) {
    case avro::AVRO_STRING:
        return "NSString *";
    case avro::AVRO_BYTES:
        return "NSData *";
    // represent all the primitives as an NSNumber so that everything is an object/pointer
    case avro::AVRO_INT:
    case avro::AVRO_LONG:
    case avro::AVRO_FLOAT:
    case avro::AVRO_DOUBLE:
    case avro::AVRO_BOOL:
        return "NSNumber *";
    case avro::AVRO_RECORD:
    case avro::AVRO_ENUM:
        {
            return decorate(n->name());
            //return inNamespace_ ? nm : fullname(nm);
        }
    case avro::AVRO_ARRAY:
        return "NSArray *";
    case avro::AVRO_MAP:
        return "NSDictionary *";
    case avro::AVRO_FIXED:
        return "NSData *";
    case avro::AVRO_SYMBOLIC:
        return objcTypeOf(resolveSymbol(n));
    default:
        return "$Undefined$";
    }
}

string CodeGen::cppTypeOf(const NodePtr& n)
{
    switch (n->type()) {
        case avro::AVRO_STRING:
            return "std::string";
        case avro::AVRO_BYTES:
            return "std::vector<uint8_t>";
        case avro::AVRO_INT:
            return "int32_t";
        case avro::AVRO_LONG:
            return "int64_t";
        case avro::AVRO_FLOAT:
            return "float";
        case avro::AVRO_DOUBLE:
            return "double";
        case avro::AVRO_BOOL:
            return "bool";
        case avro::AVRO_RECORD:
        case avro::AVRO_ENUM:
            {
                string nm = decorate(n->name());
                return inNamespace_ ? nm : fullname(nm);
            }
        case avro::AVRO_ARRAY:
            return "std::vector<" + cppTypeOf(n->leafAt(0)) + " >";
        case avro::AVRO_MAP:
            return "std::map<std::string, " + cppTypeOf(n->leafAt(1)) + " >";
        case avro::AVRO_FIXED:
            return "boost::array<uint8_t, " +
            lexical_cast<string>(n->fixedSize()) + ">";
        case avro::AVRO_SYMBOLIC:
            return cppTypeOf(resolveSymbol(n));
        default:
            return "$Undefined$";
    }
}

static string objcNameOf(const NodePtr& n)
{
    switch (n->type()) {
    case avro::AVRO_NULL:
        return "null";
    case avro::AVRO_STRING:
        return "string";
    case avro::AVRO_BYTES:
        return "bytes";
    case avro::AVRO_INT:
        return "int";
    case avro::AVRO_LONG:
        return "long";
    case avro::AVRO_FLOAT:
        return "float";
    case avro::AVRO_DOUBLE:
        return "double";
    case avro::AVRO_BOOL:
        return "bool";
    case avro::AVRO_RECORD:
    case avro::AVRO_ENUM:
    case avro::AVRO_FIXED:
        return decorate(n->name());
    case avro::AVRO_ARRAY:
        return "array";
    case avro::AVRO_MAP:
        return "map";
    case avro::AVRO_SYMBOLIC:
        return objcNameOf(resolveSymbol(n));
    default:
        return "$Undefined$";
    }
}

string CodeGen::generateRecordType(const NodePtr& n)
{
    size_t c = n->leaves();
    vector<string> types;
    for (size_t i = 0; i < c; ++i) {
        types.push_back(generateType(n->leafAt(i)));
    }

    map<NodePtr, string>::const_iterator it = done.find(n);
    if (it != done.end()) {
        return it->second;
    }

    // forward declaration
    os_ << "struct " << decorate(n->name()) << ";\n\n"
    // appending "Object" to every class
    << "@interface " << decorate(n->name()) << "Object : NSObject {\n"    
    << "}\n\n";
/*    
    if (! noUnion_) {
        for (size_t i = 0; i < c; ++i) {
            if (n->leafAt(i)->type() == avro::AVRO_UNION) {
                os_ << "typedef " << types[i]
                    << ' ' << n->nameAt(i) << "_t;\n";
            }
        }
    }
 */
    for (size_t i = 0; i < c; ++i) {
        if (! noUnion_ && n->leafAt(i)->type() == avro::AVRO_UNION) {
            os_ << "@property (nonatomic, retain, readonly) " << types[i] << " *";
        } else if (n->leafAt(i)->type() == avro::AVRO_ENUM) {
            // spit out a generated property that we'll implement
            os_ << "@property (nonatomic, assign, readonly) enum " << types[i] << "Enum ";
        } else if (n->leafAt(i)->type() == avro::AVRO_SYMBOLIC) {
            NodePtr resolved = resolveSymbol(n->leafAt(i));
            if (resolved->type() == avro::AVRO_ENUM) {
                os_ << "@property (nonatomic, assign, readonly) enum " << types[i] << "Enum ";
            } else {
                os_ << "@property (nonatomic, retain, readonly) " << types[i] << " *";
            }
        } else if (n->leafAt(i)->type() == avro::AVRO_RECORD) {
            os_ << "@property (nonatomic, retain, readonly) " << types[i] << " *";
        } else {
            os_ << "@property (nonatomic, retain, readonly) " << types[i];
        }
        os_ << n->nameAt(i);
        os_ << ";\n";
    }
    os_ << "\n"
    << "- (id)initWithStruct:(struct " << decorate(n->name()) << ")cppStruct;\n";
    os_ << "@end\n\n";
    return decorate(n->name()) + "Object";
}

void makeCanonical(string& s, bool foldCase)
{
    for (string::iterator it = s.begin(); it != s.end(); ++it) {
        if (isalpha(*it)) {
            if (foldCase) {
                *it = toupper(*it);
            }
        } else if (! isdigit(*it)) {
            *it = '_';
        }
    }
}

string CodeGen::unionName()
{
    string s = schemaFile_;
    string::size_type n = s.find_last_of("/\\");
    if (n != string::npos) {
        s = s.substr(n);
    }
    makeCanonical(s, false);
    // move the increment out
    return s + "_Union__" + boost::lexical_cast<string>(unionNumber_) + "__";
}

string CodeGen::objcUnionName() 
{
    string s = schemaFile_;
    // TODO: cut off the period
    string::size_type n = s.find_last_of("/\\");
    if (n != string::npos) {
        s = s.substr(n);
    }
    makeCanonical(s, false);
    
    return s + "_UnionObject__" + boost::lexical_cast<string>(unionNumber_) + "__";    
}

static string cppNameFromObjcName(const string& objcName) 
{
    string cppName(objcName);
    // find the last instance of the word Object, which is appended in type generation
    size_t pos = cppName.rfind("Object");
    if (pos != string::npos) {
        return cppName.erase(pos, 6);
    }
    return cppName;
}

/**
 * Generates a type that wraps a union but not the implementation
 */

string CodeGen::generateUnionType(const NodePtr& n)
{
    size_t c = n->leaves();
    vector<string> types;
    vector<string> names;
    
    set<NodePtr>::const_iterator it = doing.find(n);
    if (it != doing.end()) {
        for (size_t i = 0; i < c; ++i) {
            const NodePtr& nn = n->leafAt(i);
            types.push_back(generateDeclaration(nn));
            names.push_back(objcNameOf(nn));
        }
    } else {
        doing.insert(n);
        for (size_t i = 0; i < c; ++i) {
            const NodePtr& nn = n->leafAt(i);
            types.push_back(generateType(nn));
            names.push_back(objcNameOf(nn));
        }
        doing.erase(n);
    }
    if (done.find(n) != done.end()) {
        return done[n];
    }
    
    const string result = unionName();
    const string objcName = objcUnionName();
    // increment unionnumber
    unionNumber_++;

    os_ << "struct " << result << ";"
    << "\n"
    << "@interface " << objcName << " : NSObject {\n"
    << "@private\n"
    << "    size_t _idx;\n"
    << "    id _value;\n"
    << "}\n"
    << "\n"
    << "@property (nonatomic, assign, readonly) size_t idx;\n";
    
    for (size_t i = 0; i < c; ++i) {
        const NodePtr& nn = n->leafAt(i);
        if (nn->type() == avro::AVRO_NULL) {
            os_ << "@property (nonatomic, assign, readonly) BOOL isNull;\n";
        } else {
            const string& type = types[i];
            const string& name = names[i];
            // append "Value" to end of each type in the union since it names a type, not a variable
            if (nn->type() == avro::AVRO_ENUM) {
                // add a space for enum types and use assign
                os_ << "@property (nonatomic, assign, readonly) enum " << type << " " << name << "Value;\n";
            } else if (nn->type() == avro::AVRO_SYMBOLIC || nn->type() == avro::AVRO_RECORD) {
                // add a " *" for named types                
                os_ << "@property (nonatomic, retain, readonly) " << type << " *" << name << "Value;\n";
            } else {
                os_ << "@property (nonatomic, retain, readonly) " << type << name << "Value;\n";
            }
        }
    }
    os_ << "\n"
    << "- (id)initWithStruct:(struct " << result << ")cppStruct;\n";
    os_ << "@end\n\n";
    // return the objcName
    return objcName;
}

/**
 * Returns the type for the given schema node and emits code to os.
 */
string CodeGen::generateType(const NodePtr& n)
{
    NodePtr nn = (n->type() == avro::AVRO_SYMBOLIC) ?  resolveSymbol(n) : n;

    map<NodePtr, string>::const_iterator it = done.find(nn);
    if (it != done.end()) {
        return it->second;
    }
    string result = doGenerateType(nn);
    done[nn] = result;
    return result;
}

string CodeGen::doGenerateType(const NodePtr& n)
{
    switch (n->type()) {
    case avro::AVRO_STRING:
    case avro::AVRO_BYTES:
    case avro::AVRO_INT:
    case avro::AVRO_LONG:
    case avro::AVRO_FLOAT:
    case avro::AVRO_DOUBLE:
    case avro::AVRO_BOOL:
    case avro::AVRO_NULL:
    case avro::AVRO_FIXED:
        return objcTypeOf(n);
    case avro::AVRO_ARRAY: {
        // NOTE: cpp has the call to generateType() inside the cpp template param
        generateType(n->leafAt(0));
        return "NSArray *";
    }
    case avro::AVRO_MAP: {
        // NOTE: cpp has the call to generateType() inside the cpp template param
        generateType(n->leafAt(0));
        return "NSDictionary *";
    }
    case avro::AVRO_RECORD:
        return generateRecordType(n);
    case avro::AVRO_ENUM:
        return generateEnumType(n);
    case avro::AVRO_UNION:
        return generateUnionType(n);
    default:
        break;
    }
    return "$Undefuned$";
}

string CodeGen::generateDeclaration(const NodePtr& n)
{
    NodePtr nn = (n->type() == avro::AVRO_SYMBOLIC) ?  resolveSymbol(n) : n;
    switch (nn->type()) {
    case avro::AVRO_STRING:
    case avro::AVRO_BYTES:
    case avro::AVRO_INT:
    case avro::AVRO_LONG:
    case avro::AVRO_FLOAT:
    case avro::AVRO_DOUBLE:
    case avro::AVRO_BOOL:
    case avro::AVRO_NULL:
    case avro::AVRO_FIXED:
        return objcTypeOf(nn);
    case avro::AVRO_ARRAY:
        return "NSArray *";
    case avro::AVRO_MAP:
        return "NSDictionary *";
    case avro::AVRO_RECORD:
        os_ << "struct " << objcTypeOf(nn) << ";\n";
        return objcTypeOf(nn);
    case avro::AVRO_ENUM:
        return generateEnumType(nn);
    case avro::AVRO_UNION:
        // FIXME: When can this happen?
        return generateUnionType(nn);
    default:
        break;
    }
    return "$Undefuned$";
}

void CodeGen::generateEnumImplementation(const NodePtr& n)
{
    // nothing to do here as long as the enum values stay the same
}

void CodeGen::generateRecordImplementation(const NodePtr& n)
{
    size_t c = n->leaves();
    
    for (size_t i = 0; i < c; ++i) {
        const NodePtr& nn = n->leafAt(i);
        generateImplementation(nn);
    }
    
    string name = done[n];
    string fn = fullname(name);
    string cppRecord = cppNameFromObjcName(fn);
    
    os_ << "@implementation " << name << "\n\n";
    
    // synthesize implementations
    for (size_t i = 0; i < c; ++i) {
        os_ << "@synthesize " << n->nameAt(i) << " = _" << n->nameAt(i) << ";\n";
    }
    // constructor implementation
    os_ << "\n"
        << "- (id)initWithStruct:(struct " << cppRecord  << ")cppStruct\n"
        << "{\n"
        << "    self = [super init];\n"
        << "    if (self) {\n";
    for (size_t i = 0; i < c; ++i) {
        const NodePtr& nn = n->leafAt(i);
        const string& nameAt = n->nameAt(i);
        if (nn->type() == avro::AVRO_NULL) {
            os_ << "        _" << nameAt << " = nil;\n";
        } else if (nn->type() == avro::AVRO_STRING) {
            // give each one a unique name
            os_ << "        std::string " << nameAt << "String = cppStruct." << nameAt << ";\n"
                << "        _" << nameAt << " = " << generateObjcInitializer(nn, nameAt + "String") << ";\n";
        } else if (nn->type() == avro::AVRO_BYTES) {
            os_ << "        std::vector<uint8_t> " << nameAt << "Bytes = cppStruct." << nameAt << ";\n"
                << "        _" << nameAt << " = " << generateObjcInitializer(nn, "cppBytes") << ";\n";                
        } else if (nn->type() == avro::AVRO_INT) {
            os_ << "        _" << nameAt << " = " << generateObjcInitializer(nn, "cppStruct." + nameAt) << ";\n";
        } else if (nn->type() == avro::AVRO_LONG) {
            os_ << "        _" << nameAt << " = " << generateObjcInitializer(nn, "cppStruct." + nameAt) << ";\n";
        } else if (nn->type() == avro::AVRO_FLOAT) {
            os_ << "        _" << nameAt << " = " << generateObjcInitializer(nn, "cppStruct." + nameAt) << ";\n";
        } else if (nn->type() == avro::AVRO_DOUBLE) {
            os_ << "        _" << nameAt << " = " << generateObjcInitializer(nn, "cppStruct." + nameAt) << ";\n";
        } else if (nn->type() == avro::AVRO_BOOL) {
            os_ << "        _" << nameAt << " = " << generateObjcInitializer(nn, "cppStruct." + nameAt) << ";\n";
        } else if (nn->type() == avro::AVRO_ARRAY) {
            // the array has a single leaf that is the element type
            // IPHONE-300 give each array a unique name
            const NodePtr& element = nn->leafAt(0);
            string obcjType = objcTypeOf(element) + "Object";
            string cppType = cppTypeOf(element);
            string cppArrayName = "cpp" + nameAt + "Array";
            string objcArrayName = "objc" + nameAt + "Array";
            os_ << "        std::vector< " << cppType << " > " << cppArrayName << " = cppStruct." << nameAt << ";\n" 
                << "        NSMutableArray *" << objcArrayName << " = " << generateObjcInitializer(nn, cppArrayName) << ";\n"
                << "        for (std::vector<" << cppType << " >::const_iterator it = " << cppArrayName << ".begin(); it != " << cppArrayName << ".end(); ++it) {\n"
                << "            [" << objcArrayName << " addObject:" << generateObjcInitializer(element, "*it") << "];\n"
                << "        }\n"
                << "        _" << nameAt << " = " << objcArrayName << ";\n";
        } else if (nn->type() == avro::AVRO_MAP) {
            
            
            const NodePtr& element = nn->leafAt(1);
            string obcjType = objcTypeOf(element) + "Object";
            string cppType = cppTypeOf(element);
            string cppMapName = "cpp" + nameAt + "Map";
            string objcMapName = "objc" + nameAt + "Map";
            os_ << "        std::map<std::string, " << cppType << " > " << cppMapName << " = cppStruct." << nameAt << ";\n"
                << "        NSMutableDictionary *" << objcMapName << " = " << generateObjcInitializer(nn, cppMapName) << ";\n"
                << "        for (std::map<std::string, " << cppType << " >::const_iterator it = " << cppMapName << ".begin(); it != " << cppMapName << ".end(); ++it) {\n"
                << "        NSString *mapKey = ((__bridge_transfer NSString *)CFStringCreateWithBytes(kCFAllocatorDefault, (const UInt8 *)(((*it).first).data()), ((*it).first).size(), kCFStringEncodingUTF8, false));\n"
                << "            [" << objcMapName << " setObject:" << generateObjcInitializer(element, "(*it).second") << " forKey:mapKey" << "];\n"
                << "        }\n"
                << "        _" << nameAt << " = " << objcMapName << ";\n";

            
        } else if (nn->type() == avro::AVRO_RECORD) {
            os_ << "        _" << nameAt << " = " << generateObjcInitializer(nn, "cppStruct." + nameAt) << ";\n";
        } else if (nn->type() == avro::AVRO_SYMBOLIC) {
            const NodePtr &resolved = resolveSymbol(nn);
            os_ << "        _" << nameAt << " = " << generateObjcInitializer(resolved, "cppStruct." + nameAt) << ";\n";
        } else if (nn->type() == avro::AVRO_ENUM) {
            // cast one enum to another
            os_ << "        _" << nameAt << " = (" << decorate(nn->name()) << "Enum) cppStruct." << nameAt << ";\n";
        } else if (nn->type() == avro::AVRO_UNION) {
            os_ << "        _" << nameAt << " = [[" << fullname(done[nn]) << " alloc] initWithStruct:cppStruct." << cppNameFromObjcName(nameAt) << "];\n";
        } else {
            os_ << "#warning unknown type: " << nn->type() << "\n";
        }
    }
    os_ << "    }\n"
        << "    return self;\n"
        << "}\n\n"
        << "@end\n\n";
}

string CodeGen::generateObjcInitializer(const NodePtr& node, const string& cppValue)
{
    if (node->type() == avro::AVRO_NULL) {
        return "nil";
    } else if (node->type() == avro::AVRO_STRING) {
        // encase those cpp values in loving parens
        return "((__bridge_transfer NSString *)CFStringCreateWithBytes(kCFAllocatorDefault, (const UInt8 *)((" + cppValue + ").data()), (" + cppValue + ").size(), kCFStringEncodingUTF8, false))";
    } else if (node->type() == avro::AVRO_BYTES) {
        // encase those cpp values in loving parens
        return "((__bridge_transfer NSData *)CFDataCreate(kCFAllocatorDefault, (const UInt8 *)((" + cppValue + ").data()), (" + cppValue + ").size()))";
    } else if (node->type() == avro::AVRO_INT) {
        return "[NSNumber numberWithInt:" + cppValue + "]";
    } else if (node->type() == avro::AVRO_LONG) {
        // IPHONE-294 init with longlong
        return "[NSNumber numberWithLongLong:" + cppValue + "]";
    } else if (node->type() == avro::AVRO_FLOAT) {
        return "[NSNumber numberWithFloat:" + cppValue + "]";
    } else if (node->type() == avro::AVRO_DOUBLE) {
        return "[NSNumber numberWithDouble:" + cppValue + "]";
    } else if (node->type() == avro::AVRO_BOOL) {
        return "[NSNumber numberWithBool:" + cppValue + "]";
    } else if (node->type() == avro::AVRO_ARRAY) {
        return "[NSMutableArray arrayWithCapacity:(" + cppValue + ").size()]";
    } else if (node->type() == avro::AVRO_MAP) {
        return "[[NSMutableDictionary alloc] init]";
    } else if (node->type() == avro::AVRO_RECORD) {
        // append "Object" to end of name for objc-type, and "struct" to front of name for cpp type, and "get_" for getter
        const string &name = objcTypeOf(node);
        return "[[" + name + "Object alloc] initWithStruct:" + cppValue + "]";
    } else if (node->type() == avro::AVRO_FIXED) {
        return "CFDataCreate(kCFAllocatorDefault, " + cppValue + ".data(), " + cppValue + ".size())";
    } else if (node->type() == avro::AVRO_SYMBOLIC) {
        return generateObjcInitializer(resolveSymbol(node), cppValue);
    } else if (node->type() == avro::AVRO_UNION) {
        // append "Object" to end of name for objc-type, and "struct" to front of name for cpp type, and "get_" for getter
        const string &name = objcTypeOf(node);
        return "[[" + name + "Object alloc] initWithStruct:" + cppValue + "]";
    } else if (node->type() == avro::AVRO_ENUM) {
        return "(" + decorate(node->name()) + "Enum) " + cppValue + ";";
    }
    // don't deal with enum?
    return "";
}

void CodeGen::generateUnionImplementation(const NodePtr& n)
{
    size_t c = n->leaves();

    for (size_t i = 0; i < c; ++i) {
        const NodePtr& nn = n->leafAt(i);
        generateImplementation(nn);
    }

    string name = done[n];
    string fn = fullname(name);
    string cppUnion = cppNameFromObjcName(fn);

    os_ << "@implementation " << fn << "\n\n";
    
    // getter implementations
    for (size_t i = 0; i < c; ++i) {
        const NodePtr& nn = n->leafAt(i);
        if (nn->type() == avro::AVRO_NULL) {
            os_ << "- (BOOL)isNull\n"
                << "{\n"
                << "    " << "return _idx == " << i << ";\n"
                << "}\n\n";
        } else {
            string type = objcTypeOf(nn);
            string attrName = objcNameOf(nn);
            string pointer = (nn->type() == avro::AVRO_RECORD || nn->type() == avro::AVRO_SYMBOLIC) ? "Object *" : "";
            os_ << "- (" <<  type << pointer << ")" << attrName << "Value\n"
                << "{\n"
                << "    if (_idx != " << i << ") {\n"
                << "        return nil;\n"
                << "    }\n"
                << "    return (" << type << pointer << ")" << "_value;\n"
                << "}\n\n";
        }
    }
    // index getter
    os_ << "- (size_t)idx\n"
        << "{\n"
        << "    return _idx;\n"
        << "}\n\n";

    // constructor implementation
    os_ << "- (id)initWithStruct:(struct " << cppUnion  << ")cppStruct\n"
        << "{\n"
        << "    self = [super init];\n"
        << "    if (self) {\n"
        << "        _idx = cppStruct.idx();\n"
        << "        // now set the value based on the named of the type in the union\n"
        << "        switch(_idx) {\n";
    for (size_t i = 0; i < c; ++i) {
        const NodePtr& nn = n->leafAt(i);
        os_ << "            case " << i << ": {\n";
        if (nn->type() == avro::AVRO_NULL) {
            os_ << "                 _value = nil;\n";
        } else if (nn->type() == avro::AVRO_STRING) {
            os_ << "                 std::string cppString = cppStruct.get_string();\n"
                << "                 _value = " << generateObjcInitializer(nn, string("cppString")) << ";\n";
        } else if (nn->type() == avro::AVRO_BYTES) {
            os_ << "                 std::vector<uint8_t> cppBytes = cppStruct.get_bytes();\n"
                << "                 _value = " << generateObjcInitializer(nn, string("cppBytes")) << ";\n";
        } else if (nn->type() == avro::AVRO_INT) {
            os_ << "                 _value = " << generateObjcInitializer(nn, "cppStruct.get_int()") << ";\n";
        } else if (nn->type() == avro::AVRO_LONG) {
            os_ << "                 _value = " << generateObjcInitializer(nn, "cppStruct.get_long()") << ";\n";
        } else if (nn->type() == avro::AVRO_FLOAT) {
            os_ << "                 _value = " << generateObjcInitializer(nn, "cppStruct.get_float()") << ";\n";
        } else if (nn->type() == avro::AVRO_DOUBLE) {
            os_ << "                 _value = " << generateObjcInitializer(nn, "cppStruct.get_double()") << ";\n";
        } else if (nn->type() == avro::AVRO_BOOL) {
            os_ << "                 _value = " << generateObjcInitializer(nn, "cppStruct.get_bool()") << ";\n";
        } else if (nn->type() == avro::AVRO_ARRAY) {
            // the array has a single leaf that is the element type
            const NodePtr& element = nn->leafAt(0);
            string obcjType = objcTypeOf(element) + "Object";
            string cppType = cppTypeOf(element);
            os_ << "                 std::vector<" << cppType << "> cppArray = cppStruct.get_array();\n" 
                << "                 NSMutableArray *array = " << generateObjcInitializer(nn, "cppArray") << ";\n"
                << "                 for (std::vector<" << cppType << ">::const_iterator it = cppArray.begin(); it != cppArray.end(); ++it) {\n"
                << "                     [array addObject:" << generateObjcInitializer(element, "*it") << "];\n"
                << "                 }\n"
                << "                 _value = array;\n";
        } else if (nn->type() == avro::AVRO_MAP) {
            os_ << "#warning incomplete implementation\n"
                << "                 _value = [[NSMutableDictionary alloc] init];\n";
        } else if (nn->type() == avro::AVRO_RECORD) {
            os_ << "                 _value = " << generateObjcInitializer(nn, "cppStruct.get_" + decorate(nn->name()) + "()") << ";\n";
        } else if (nn->type() == avro::AVRO_SYMBOLIC) {
            const NodePtr &resolved = resolveSymbol(nn);
            os_ << "                 _value = " << generateObjcInitializer(resolved, "cppStruct.get_" + decorate(resolved->name()) + "()") << ";\n";
        } else if (nn->type() == avro::AVRO_ENUM) {
            os_ << "                 _value = cppStruct.get_" + decorate(nn->name()) + "();\n";
        } else {
            os_ << "#warning unknown type: " << nn->type() << "\n";
        }
        os_ << "                 break;\n";
        os_ << "            }\n";
    }
    os_ << "        }\n"
        << "    }\n"
        << "    return self;\n";
    os_ << "}\n\n";
    os_ << "@end\n\n";
}

void CodeGen::generateImplementation(const NodePtr& n)
{
    switch (n->type()) {
    case avro::AVRO_STRING:
    case avro::AVRO_BYTES:
    case avro::AVRO_INT:
    case avro::AVRO_LONG:
    case avro::AVRO_FLOAT:
    case avro::AVRO_DOUBLE:
    case avro::AVRO_BOOL:
    case avro::AVRO_NULL:
        break;
    case avro::AVRO_RECORD:
        generateRecordImplementation(n);
        break;
    case avro::AVRO_ENUM:
        generateEnumImplementation(n);
        break;
    case avro::AVRO_ARRAY:
    case avro::AVRO_MAP:
        generateImplementation(n->leafAt(n->type() == avro::AVRO_ARRAY ? 0 : 1));
        break;
    case avro::AVRO_UNION:
        generateUnionImplementation(n);
        break;
    case avro::AVRO_FIXED:
        break;
    default:
        break;
    }
}

void CodeGen::emitCopyright()
{
    os_ << 
        "/**\n"
        " * Licensed to the Apache Software Foundation (ASF) under one\n"
        " * or more contributor license agreements.  See the NOTICE file\n"
        " * distributed with this work for additional information\n"
        " * regarding copyright ownership.  The ASF licenses this file\n"
        " * to you under the Apache License, Version 2.0 (the\n"
        " * \"License\"); you may not use this file except in compliance\n"
        " * with the License.  You may obtain a copy of the License at\n"
        " *\n"
        " *     http://www.apache.org/licenses/LICENSE-2.0\n"
        " *\n"
        " * Unless required by applicable law or agreed to in writing, "
            "software\n"
        " * distributed under the License is distributed on an "
            "\"AS IS\" BASIS,\n"
        " * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express "
            "or implied.\n"
        " * See the License for the specific language governing "
            "permissions and\n"
        " * limitations under the License.\n"
        " */\n\n\n";
}

string CodeGen::guard()
{
    string h = headerFile_;
    makeCanonical(h, true);
    return h + "_" + lexical_cast<string>(random_()) + "__H_";
}

void CodeGen::generate(const ValidSchema& schema)
{
    emitCopyright();

    os_ << "#import <Foundation/Foundation.h>\n"
        << "\n";

    if (! ns_.empty()) {
        // set the flag, but obj-c doesn't support namespaces. Only used for 
        // referring to the proper cpp type
        inNamespace_ = true;
    }

    const NodePtr& root = schema.root();
    generateType(root);

    if (! ns_.empty()) {
        inNamespace_ = false;
    }
    
    // output the implementation
    os_ << "/* AUTO-GENERATED BY AVROGENOBJC -- DO NOT EDIT */\n\n";
    os_ << "#import <CoreFoundation/CoreFoundation.h>\n";
    string canonical(headerFile_);
    makeCanonical(canonical, false);
    os_ << "#import \"" << canonical << "\"\n";
    // include .hh file too
    os_ << "#import \"" << canonical << "h\"\n\n";
    
    generateImplementation(root);
    
    os_ << "/* END AUTO-GENERATED CODE */\n\n";
    
    os_.flush();

}

namespace po = boost::program_options;

static const string NS("namespace");
static const string OUT("output");
static const string IN("input");
static const string INCLUDE_PREFIX("include-prefix");
static const string NO_UNION_TYPEDEF("no-union-typedef");
static const string IMPLEMENTATION("implementation");

int main(int argc, char** argv)
{
    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("include-prefix,p", po::value<string>()->default_value("avro"),
            "prefix for include headers, - for none, default: avro")
        ("no-union-typedef,U", "do not generate typedefs for unions in records")
        ("namespace,n", po::value<string>(), "set namespace for generated code")
        ("input,i", po::value<string>(), "input file")
        ("output,o", po::value<string>(), "output file to generate")
        ("implementation,I", po::value<string>(), "generate the .m implementation classes");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);


    if (vm.count("help") || vm.count(IN) == 0 || vm.count(OUT) == 0) {
        std::cout << desc << std::endl;
        return 1;
    }

    string ns = vm.count(NS) > 0 ? vm[NS].as<string>() : string();
    string outf = vm.count(OUT) > 0 ? vm[OUT].as<string>() : string();
    string inf = vm.count(IN) > 0 ? vm[IN].as<string>() : string();
    bool impl = vm.count(IMPLEMENTATION) != 0;
    string incPrefix = vm[INCLUDE_PREFIX].as<string>();
    bool noUnion = vm.count(NO_UNION_TYPEDEF) != 0;
    if (incPrefix == "-") {
        incPrefix.clear();
    } else if (*incPrefix.rbegin() != '/') {
        incPrefix += "/";
    }

    try {
        ValidSchema schema;

        if (! inf.empty()) {
            ifstream in(inf.c_str());
            compileJsonSchema(in, schema);
        } else {
            compileJsonSchema(std::cin, schema);
        }

        if (! outf.empty()) {
            ofstream out(outf.c_str());
            CodeGen(out, ns, inf, outf, incPrefix, noUnion, impl).generate(schema);
        } else {
            CodeGen(std::cout, ns, inf, outf, incPrefix, noUnion, impl).
            generate(schema);
        }            
        
        return 0;
    } catch (std::exception &e) {
        std::cerr << "Failed to parse or compile schema: "
            << e.what() << std::endl;
        return 1;
    }

}
