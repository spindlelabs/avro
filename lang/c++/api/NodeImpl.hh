/*
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

#ifndef avro_NodeImpl_hh__
#define avro_NodeImpl_hh__

#include <limits>
#include <set>
#include <boost/weak_ptr.hpp>

#include "Config.hh"
#include "Node.hh"
#include "NodeConcepts.hh"

namespace avro {

/// Implementation details for Node.  NodeImpl represents all the avro types,
/// whose properties are enabled are disabled by selecting concept classes.

template 
< 
    class NameConcept,
    class LeavesConcept,
    class LeafNamesConcept,
    class SizeConcept,
    class NamespaceConcept
>
class NodeImpl : public Node
{

  protected:

    NodeImpl(Type type) :
        Node(type),
        nameAttribute_(),
        leafAttributes_(),
        leafNameAttributes_(),
        sizeAttribute_(),
        namespaceAttribute_()
    { }

    NodeImpl(Type type, 
             const NameConcept &name, 
             const LeavesConcept &leaves, 
             const LeafNamesConcept &leafNames,
             const SizeConcept &size,
             const NamespaceConcept &ns
             ) :
    Node(type),
    nameAttribute_(name),
    leafAttributes_(leaves),
    leafNameAttributes_(leafNames),
    sizeAttribute_(size),
    namespaceAttribute_(ns)
    { }

    void swap(NodeImpl& impl) {
        std::swap(nameAttribute_, impl.nameAttribute_);
        std::swap(leafAttributes_, impl.leafAttributes_);
        std::swap(leafNameAttributes_, impl.leafNameAttributes_);
        std::swap(sizeAttribute_, impl.sizeAttribute_);
        std::swap(nameIndex_, impl.nameIndex_);
		std::swap(namespaceAttribute_, impl.namespaceAttribute_);
    }
    bool hasName() const {
        return NameConcept::hasAttribute;
    }

    void doSetName(const std::string &name) {
        nameAttribute_.add(name);
    }
    
    const std::string &name() const {
        return nameAttribute_.get();
    }

    bool hasNamespace() const {
        return NamespaceConcept::hasAttribute;
    }
    
    void doSetNamespace(const std::string &ns) {
        namespaceAttribute_.add(ns);
    }
    
    const std::string &getNamespace() const {
        return namespaceAttribute_.get();
    }

    void doAddLeaf(const NodePtr &newLeaf) { 
        leafAttributes_.add(newLeaf);
    }

    size_t leaves() const {
        return leafAttributes_.size();
    }

    const NodePtr &leafAt(int index) const { 
        return leafAttributes_.get(index);
    }

    void doAddName(const std::string &name) { 
        if(! nameIndex_.add(name, leafNameAttributes_.size())) {
            throw Exception(boost::format("Cannot add duplicate name: %1%") % name);
        }
        leafNameAttributes_.add(name);
    }

    size_t names() const {
        return leafNameAttributes_.size();
    }

    const std::string &nameAt(int index) const { 
        return leafNameAttributes_.get(index);
    }

    bool nameIndex(const std::string &name, size_t &index) const {
        return nameIndex_.lookup(name, index);
    }

    void doSetFixedSize(int size) {
        sizeAttribute_.add(size);
    }

    int fixedSize() const {
        return sizeAttribute_.get();
    }

    virtual bool isValid() const = 0;

    void printBasicInfo(std::ostream &os) const;

    void setLeafToSymbolic(int index, const NodePtr &node);
   
    SchemaResolution furtherResolution(const Node &node) const;


    NameConcept nameAttribute_;
    LeavesConcept leafAttributes_;
    LeafNamesConcept leafNameAttributes_;
    SizeConcept sizeAttribute_;
    NamespaceConcept namespaceAttribute_;
    concepts::NameIndexConcept<LeafNamesConcept> nameIndex_;
};

typedef concepts::NoAttribute<std::string>     NoName;
typedef concepts::SingleAttribute<std::string> HasName;
typedef concepts::NoAttribute<std::string>     NoNamespace;
typedef concepts::SingleAttribute<std::string> HasNamespace;

typedef concepts::NoAttribute<NodePtr>      NoLeaves;
typedef concepts::SingleAttribute<NodePtr>  SingleLeaf;
typedef concepts::MultiAttribute<NodePtr>   MultiLeaves;

typedef concepts::NoAttribute<std::string>     NoLeafNames;
typedef concepts::MultiAttribute<std::string>  LeafNames;

typedef concepts::NoAttribute<int>     NoSize;
typedef concepts::SingleAttribute<int> HasSize;

typedef NodeImpl< NoName,  NoLeaves,    NoLeafNames,  NoSize,  NoNamespace > NodeImplPrimitive;
typedef NodeImpl< HasName, NoLeaves,    NoLeafNames,  NoSize,  NoNamespace > NodeImplSymbolic;
// Modify all the named types (record, enum, fixed) to take a namespace . It would have
// been nice to group namespace and name but I didn't want to break other code that relied
// on template parameter order
typedef NodeImpl< HasName, MultiLeaves, LeafNames,    NoSize,  HasNamespace > NodeImplRecord;
typedef NodeImpl< HasName, NoLeaves,    LeafNames,    NoSize,  HasNamespace > NodeImplEnum;
typedef NodeImpl< NoName,  SingleLeaf,  NoLeafNames,  NoSize,  NoNamespace > NodeImplArray;
typedef NodeImpl< NoName,  MultiLeaves, NoLeafNames,  NoSize,  NoNamespace> NodeImplMap;
typedef NodeImpl< NoName,  MultiLeaves, NoLeafNames,  NoSize,  NoNamespace > NodeImplUnion;
typedef NodeImpl< HasName, NoLeaves,    NoLeafNames,  HasSize, HasNamespace > NodeImplFixed;

class AVRO_DECL NodePrimitive : public NodeImplPrimitive
{
  public:

    explicit NodePrimitive(Type type) :
        NodeImplPrimitive(type)
    { }

    SchemaResolution resolve(const Node &reader)  const;

    void printJson(std::ostream &os, int depth) const;

    bool isValid() const {
        return true;
    }
};

class AVRO_DECL NodeSymbolic : public NodeImplSymbolic
{
    typedef boost::weak_ptr<Node> NodeWeakPtr;

  public:

    NodeSymbolic() :
        NodeImplSymbolic(AVRO_SYMBOLIC)
    { }

    explicit NodeSymbolic(const HasName &name) :
        NodeImplSymbolic(AVRO_SYMBOLIC, name, NoLeaves(), NoLeafNames(), NoSize(), NoNamespace())
    { }

    NodeSymbolic(const HasName &name, const NodePtr n) :
        NodeImplSymbolic(AVRO_SYMBOLIC, name, NoLeaves(), NoLeafNames(), NoSize(), NoNamespace()), actualNode_(n)
    { }
    SchemaResolution resolve(const Node &reader)  const;

    void printJson(std::ostream &os, int depth) const;

    bool isValid() const {
        return (nameAttribute_.size() == 1);
    }

    bool isSet() const {
         return (actualNode_.lock() != 0);
    }

    NodePtr getNode() const {
        NodePtr node = actualNode_.lock();
        if(!node) {
            throw Exception(boost::format("Could not follow symbol %1%") % name());
        }
        return node;
    }

    void setNode(const NodePtr &node) {
        actualNode_ = node;
    }

    virtual void checkName(const std::string &name) const {
        // use namespace version since a symbol may be a fullname
        checkNamespace(name);
    }

  protected:

    NodeWeakPtr actualNode_;

};

class AVRO_DECL NodeRecord : public NodeImplRecord
{
  public:

    NodeRecord() :
        NodeImplRecord(AVRO_RECORD) 
    { }

    NodeRecord(const HasName &name, const MultiLeaves &fields, const LeafNames &fieldsNames, const HasNamespace &ns) :
        NodeImplRecord(AVRO_RECORD, name, fields, fieldsNames, NoSize(), ns)
    { 
        for(size_t i=0; i < leafNameAttributes_.size(); ++i) {
            if(!nameIndex_.add(leafNameAttributes_.get(i), i)) {
                 throw Exception(boost::format("Cannot add duplicate name: %1%") % leafNameAttributes_.get(i));
            }
        }
    }

    void swap(NodeRecord& r) {
        NodeImplRecord::swap(r);
    }

    SchemaResolution resolve(const Node &reader)  const;

    void printJson(std::ostream &os, int depth) const;

    bool isValid() const {
        return (
                (nameAttribute_.size() == 1) && 
                // SEE AVRO-1065
                //(leafAttributes_.size() > 0) &&
                (leafAttributes_.size() == leafNameAttributes_.size())
               );
    }
};

class AVRO_DECL NodeEnum : public NodeImplEnum
{
  public:

    NodeEnum() :
        NodeImplEnum(AVRO_ENUM) 
    { }

    NodeEnum(const HasName &name, const LeafNames &symbols, const HasNamespace &ns) :
        NodeImplEnum(AVRO_ENUM, name, NoLeaves(), symbols, NoSize(), ns)
    { 
        for(size_t i=0; i < leafNameAttributes_.size(); ++i) {
            if(!nameIndex_.add(leafNameAttributes_.get(i), i)) {
                 throw Exception(boost::format("Cannot add duplicate name: %1%") % leafNameAttributes_.get(i));
            }
        }
    }
        
    SchemaResolution resolve(const Node &reader)  const;

    void printJson(std::ostream &os, int depth) const;

    bool isValid() const {
        return (
                (nameAttribute_.size() == 1) && 
                (leafNameAttributes_.size() > 0) 
               );
    }
};

class AVRO_DECL NodeArray : public NodeImplArray
{
  public:

    NodeArray() :
        NodeImplArray(AVRO_ARRAY)
    { }

    explicit NodeArray(const SingleLeaf &items) :
        NodeImplArray(AVRO_ARRAY, NoName(), items, NoLeafNames(), NoSize(), NoNamespace())
    { }

    SchemaResolution resolve(const Node &reader)  const;

    void printJson(std::ostream &os, int depth) const;

    bool isValid() const {
        return (leafAttributes_.size() == 1);
    }
};

class AVRO_DECL NodeMap : public NodeImplMap
{
  public:

    NodeMap() :
        NodeImplMap(AVRO_MAP)
    { 
         NodePtr key(new NodePrimitive(AVRO_STRING));
         doAddLeaf(key);
    }

    explicit NodeMap(const SingleLeaf &values) :
        NodeImplMap(AVRO_MAP, NoName(), values, NoLeafNames(), NoSize(), NoNamespace())
    { 
        // need to add the key for the map too
        NodePtr key(new NodePrimitive(AVRO_STRING));
        doAddLeaf(key);

        // key goes before value
        std::swap(leafAttributes_.get(0), leafAttributes_.get(1));
    }

    SchemaResolution resolve(const Node &reader)  const;

    void printJson(std::ostream &os, int depth) const;

    bool isValid() const {
        return (leafAttributes_.size() == 2);
    }
};

class AVRO_DECL NodeUnion : public NodeImplUnion
{
  public:

    NodeUnion() :
        NodeImplUnion(AVRO_UNION)
    { }

    explicit NodeUnion(const MultiLeaves &types) :
        NodeImplUnion(AVRO_UNION, NoName(), types, NoLeafNames(), NoSize(), NoNamespace())
    { }

    SchemaResolution resolve(const Node &reader)  const;

    void printJson(std::ostream &os, int depth) const;

    bool isValid() const {
        std::set<std::string> seen;
        if (leafAttributes_.size() >= 1) {
            for (size_t i = 0; i < leafAttributes_.size(); ++i) {
                std::string name;
                const NodePtr& n = leafAttributes_.get(i);
                switch (n->type()) {
                case AVRO_STRING:
                    name = "string";
                    break;
                case AVRO_BYTES:
                    name = "bytes";
                    break;
                case AVRO_INT:
                    name = "int";
                    break;
                case AVRO_LONG:
                    name = "long";
                    break;
                case AVRO_FLOAT:
                    name = "float";
                    break;
                case AVRO_DOUBLE:
                    name = "double";
                    break;
                case AVRO_BOOL:
                    name = "bool";
                    break;
                case AVRO_NULL:
                    name = "null";
                    break;
                case AVRO_ARRAY:
                    name = "array";
                    break;
                case AVRO_MAP:
                    name = "map";
                    break;
                case AVRO_RECORD:
                case AVRO_ENUM:
                case AVRO_UNION:
                case AVRO_FIXED:
                case AVRO_SYMBOLIC:
                    name = n->name();
                    break;
                default:
                    return false;
                }
                if (seen.find(name) != seen.end()) {
                    return false;
                }
                seen.insert(name);
            }
            return true;
        }
        return false;
    }
};

class AVRO_DECL NodeFixed : public NodeImplFixed
{
  public:

    NodeFixed() :
        NodeImplFixed(AVRO_FIXED)
    { }

    NodeFixed(const HasName &name, const HasSize &size, const HasNamespace &ns) :
        NodeImplFixed(AVRO_FIXED, name, NoLeaves(), NoLeafNames(), size, ns)
    { }

    SchemaResolution resolve(const Node &reader)  const;

    void printJson(std::ostream &os, int depth) const;

    bool isValid() const {
        return (
                (nameAttribute_.size() == 1) && 
                (sizeAttribute_.size() == 1) 
               );
    }
};

template < class A, class B, class C, class D, class E >
inline void 
NodeImpl<A,B,C,D,E>::setLeafToSymbolic(int index, const NodePtr &node)
{
    if(!B::hasAttribute) {
        throw Exception("Cannot change leaf node for nonexistent leaf");
    } 

    NodePtr &replaceNode = const_cast<NodePtr &>(leafAttributes_.get(index));
    std::string fullname;
    if (!node->getNamespace().empty()) {
        fullname.append(node->getNamespace());
        fullname.append(".");
    }
    fullname.append(node->name());
    if(replaceNode->name() != fullname) {
        throw Exception("Symbolic name does not match the name of the schema it references");
    }

    NodePtr symbol(new NodeSymbolic);
    NodeSymbolic *ptr = static_cast<NodeSymbolic *> (symbol.get());

    // set it to its full symbol name
    ptr->setName(fullname);
    ptr->setNode(node);
    replaceNode.swap(symbol);
}

template < class A, class B, class C, class D, class E >
inline void 
NodeImpl<A,B,C,D,E>::printBasicInfo(std::ostream &os) const
{
    os << type();
    /*
    if(hasNamespace() && !namespaceAttribute_.get().empty()) {
        os << " " << namespaceAttribute_.get();
    }
    */
    if(hasName()) {
        std::string n(nameAttribute_.get());
        size_t lastDot = n.rfind('.');
        if (lastDot != std::string::npos) {
            n = n.substr(lastDot + 1);
        }
        os << " " << n;
    }
    if(D::hasAttribute) {
        os << " " << sizeAttribute_.get();
    }
    os << '\n';
    int count = leaves();
    count = count ? count : names();
    for(int i= 0; i < count; ++i) {
        if( C::hasAttribute ) {
            os << "name " << nameAt(i) << '\n';
        }
        if( type() != AVRO_SYMBOLIC && leafAttributes_.hasAttribute) {
            leafAt(i)->printBasicInfo(os);
        }
    }
    if(isCompound(type())) {
        os << "end " << type() << '\n';
    }
}


inline NodePtr resolveSymbol(const NodePtr &node) 
{
    if(node->type() != AVRO_SYMBOLIC) {
        throw Exception("Only symbolic nodes may be resolved");
    }
    boost::shared_ptr<NodeSymbolic> symNode = boost::static_pointer_cast<NodeSymbolic>(node);
    return symNode->getNode();
}

} // namespace avro

#endif
